"""Card processor daily files: header, detail records and a control trailer.

Layout (pipe-delimited, format version 01)::

    H|CARDTXN|01|<processor>|<business date YYYYMMDD>|<sequence>|<created YYYYMMDDHHMMSS>
    D|<txn ref>|<card token>|<local time YYYYMMDDHHMMSS>|<type>|<channel>|<mcc>|<merchant id>|
      <merchant name>|<merchant city>|<merchant country>|<amount minor units>|<ISO 4217 numeric>|
      <response code>|<auth code>
    T|<detail record count>|<sum of detail amounts in minor units>

The trailer is the only proof that a file arrived complete. Defects seen in real
processor exchanges can be injected per business date to exercise the ingestion
controls: ``duplicates`` (records sent twice, trailer consistent with the file),
``truncated`` (transfer cut short, trailer describes the full file) and
``date-format`` (a processor release writes the local time as ``YYYY-MM-DD HH:MM:SS``
while the header still announces format version 01).
"""

from __future__ import annotations

import math
import random
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import StrEnum
from pathlib import Path

import psycopg
from psycopg import sql

from aml_platform.sources.landing import atomic_writer

FORMAT_VERSION = "01"
PROCESSOR_CODE = "PROC01"
DELIMITER = "|"
DETAIL_FIELDS: tuple[str, ...] = (
    "record_type",
    "txn_ref",
    "card_token",
    "txn_local_time",
    "txn_type",
    "channel",
    "mcc",
    "merchant_id",
    "merchant_name",
    "merchant_city",
    "merchant_country",
    "amount_minor",
    "currency",
    "response_code",
    "auth_code",
)

MAD, EUR, USD = "504", "978", "840"
DOMESTIC_MCC: tuple[tuple[str, str, float, float], ...] = (
    # mcc, merchant label, lognormal mu and sigma of the amount in MAD
    ("5411", "Superette", 4.6, 0.9),
    ("5812", "Restaurant", 5.0, 0.7),
    ("5814", "Cafe", 3.6, 0.5),
    ("5541", "Station", 5.6, 0.5),
    ("5651", "Boutique", 6.0, 0.8),
    ("5732", "Electro", 7.4, 0.9),
    ("5912", "Pharmacie", 4.8, 0.8),
    ("4111", "Transport", 3.4, 0.6),
    ("7011", "Hotel", 7.2, 0.7),
    ("5311", "Bazar", 5.5, 0.9),
)
ECOM_MCC: tuple[tuple[str, str, float, float], ...] = (
    ("5399", "Shop Online", 5.8, 1.0),
    ("4722", "Travel Booking", 7.6, 0.8),
    ("5815", "Digital Media", 4.2, 0.6),
)
FOREIGN_MARKETS: tuple[tuple[str, str], ...] = (("FRA", EUR), ("ESP", EUR), ("NLD", EUR), ("USA", USD), ("ARE", USD))
ATM_AMOUNTS_MAD: tuple[int, ...] = (100, 200, 200, 300, 500, 500, 1000, 1000, 1500, 2000, 3000, 4000)
MERCHANT_WORDS: tuple[str, ...] = ("Atlas", "Sebou", "Anfa", "Medina", "Oasis", "Argan", "Rif", "Souss", "Ziz", "Nour")


class Defect(StrEnum):
    DUPLICATES = "duplicates"
    TRUNCATED = "truncated"
    DATE_FORMAT = "date-format"


@dataclass(frozen=True)
class CardHolder:
    card_token: str
    home_city: str


@dataclass(frozen=True)
class Merchant:
    merchant_id: str
    name: str
    city: str
    country: str
    mcc: str
    currency: str
    mu: float
    sigma: float
    channel: str


@dataclass(frozen=True)
class CardFile:
    header: tuple[str, ...]
    details: list[tuple[str, ...]]
    trailer: tuple[str, ...]

    @property
    def business_date(self) -> date:
        return datetime.strptime(self.header[4], "%Y%m%d").date()

    def trailer_matches(self) -> bool:
        declared_count, declared_total = int(self.trailer[1]), int(self.trailer[2])
        return declared_count == len(self.details) and declared_total == sum(int(d[11]) for d in self.details)


def file_name(business_date: date, sequence: int = 1) -> str:
    return f"CARDTXN_{business_date:%Y%m%d}_{sequence:03d}.dat"


def load_card_holders(conninfo: str, schema: str = "core") -> list[CardHolder]:
    query = sql.SQL(
        "SELECT c.card_token, cu.city FROM {s}.card c"
        " JOIN {s}.account a ON a.account_id = c.account_id"
        " JOIN {s}.customer cu ON cu.customer_id = a.customer_id"
        " WHERE c.status = 'ACTIVE' ORDER BY c.card_id"
    ).format(s=sql.Identifier(schema))
    with psycopg.connect(conninfo) as connection, connection.cursor() as cursor:
        cursor.execute(query)
        return [CardHolder(token, city) for token, city in cursor.fetchall()]


def build_merchants(cities: Sequence[str], seed: int, per_city: int = 300) -> list[Merchant]:
    """Acceptors: ATMs and shops in each customer city, plus foreign e-commerce sites."""
    rng = random.Random(f"{seed}|merchants")
    merchants: list[Merchant] = []
    for city in sorted(set(cities)):
        for _ in range(per_city // 10):
            merchants.append(
                Merchant(f"ATM{len(merchants) + 1:05d}", f"GAB {city}"[:40], city, "MAR", "6011", MAD, 0.0, 0.0, "ATM")
            )
        for _ in range(per_city):
            mcc, label, mu, sigma = rng.choice(DOMESTIC_MCC)
            merchants.append(
                Merchant(
                    merchant_id=f"M{len(merchants) + 1:07d}",
                    name=f"{label} {rng.choice(MERCHANT_WORDS)} {city}"[:40],
                    city=city,
                    country="MAR",
                    mcc=mcc,
                    currency=MAD,
                    mu=mu,
                    sigma=sigma,
                    channel="POS",
                )
            )
    for country, currency in FOREIGN_MARKETS:
        for _ in range(per_city // 3):
            mcc, label, mu, sigma = rng.choice(ECOM_MCC)
            merchants.append(
                Merchant(
                    merchant_id=f"M{len(merchants) + 1:07d}",
                    name=f"{label} {rng.choice(MERCHANT_WORDS)}"[:40],
                    city="ONLINE",
                    country=country,
                    mcc=mcc,
                    currency=currency,
                    # Foreign amounts are in EUR/USD, roughly a tenth of MAD.
                    mu=mu - math.log(10),
                    sigma=sigma,
                    channel="ECOM",
                )
            )
    return merchants


def _amount_minor(rng: random.Random, mu: float, sigma: float) -> int:
    return max(100, round(rng.lognormvariate(mu, sigma) * 100))


def _detail_records(
    rng: random.Random,
    business_date: date,
    holders: Sequence[CardHolder],
    merchants: Sequence[Merchant],
    count: int,
) -> list[tuple[str, ...]]:
    weights = [random.Random(h.card_token).lognormvariate(0.0, 1.0) for h in holders]
    domestic: dict[str, list[Merchant]] = {}
    atms: dict[str, list[Merchant]] = {}
    online: list[Merchant] = []
    for merchant in merchants:
        if merchant.channel == "ECOM":
            online.append(merchant)
        else:
            (atms if merchant.channel == "ATM" else domestic).setdefault(merchant.city, []).append(merchant)
    all_domestic = [m for city in sorted(domestic) for m in domestic[city]]
    all_atms = [m for city in sorted(atms) for m in atms[city]]

    records: list[tuple[str, ...]] = []
    for sequence, holder in enumerate(rng.choices(holders, weights=weights, k=count), start=1):
        # A share of each file is late presentment from the previous day.
        txn_day = business_date - timedelta(days=1) if rng.random() < 0.03 else business_date
        local_time = datetime.combine(txn_day, time(0)) + timedelta(seconds=rng.randint(6 * 3600, 86_399))
        kind = rng.random()
        if kind < 0.25:
            merchant = rng.choice(atms.get(holder.home_city, all_atms) if rng.random() < 0.9 else all_atms)
            txn_type = "WDL"
            amount = rng.choice(ATM_AMOUNTS_MAD) * 100
        else:
            if kind < 0.87:
                local = domestic.get(holder.home_city, all_domestic)
                merchant = rng.choice(local if rng.random() < 0.85 else all_domestic)
            else:
                merchant = rng.choice(online)
            txn_type = "REF" if rng.random() < 0.02 else "PUR"
            amount = _amount_minor(rng, merchant.mu, merchant.sigma)
        channel = merchant.channel
        outcome = rng.random()
        if outcome < 0.95:
            response, auth = "00", f"{rng.randint(0, 999_999):06d}"
        else:
            response, auth = rng.choice(("05", "51", "55" if channel == "ATM" else "61")), ""
        records.append(
            (
                "D",
                f"{business_date:%Y%m%d}{sequence:08d}",
                holder.card_token,
                f"{local_time:%Y%m%d%H%M%S}",
                txn_type,
                channel,
                merchant.mcc,
                merchant.merchant_id,
                merchant.name,
                merchant.city,
                merchant.country,
                str(amount),
                merchant.currency,
                response,
                auth,
            )
        )
    records.sort(key=lambda record: record[3])
    return records


def generate_file(
    landing_dir: Path,
    business_date: date,
    holders: Sequence[CardHolder],
    merchants: Sequence[Merchant],
    *,
    count: int,
    seed: int,
    defects: frozenset[Defect] = frozenset(),
) -> Path:
    rng = random.Random(f"{seed}|{business_date.isoformat()}")
    details = _detail_records(rng, business_date, holders, merchants, count)
    if Defect.DUPLICATES in defects:
        resent = rng.sample(details, max(1, len(details) // 200))
        details = sorted(details + resent, key=lambda record: record[3])
    trailer = ("T", str(len(details)), str(sum(int(record[11]) for record in details)))
    if Defect.TRUNCATED in defects:
        details = details[: len(details) - rng.randint(1, max(1, len(details) // 100))]
    if Defect.DATE_FORMAT in defects:
        details = [(*d[:3], _iso_local_time(d[3]), *d[4:]) for d in details]
    created = datetime.combine(business_date + timedelta(days=1), time(2, 15))
    header = (
        "H",
        "CARDTXN",
        FORMAT_VERSION,
        PROCESSOR_CODE,
        f"{business_date:%Y%m%d}",
        "001",
        f"{created:%Y%m%d%H%M%S}",
    )

    target = landing_dir / file_name(business_date)
    with atomic_writer(target, encoding="ascii", newline="") as handle:
        handle.write(DELIMITER.join(header) + "\n")
        for record in details:
            handle.write(DELIMITER.join(record) + "\n")
        if Defect.TRUNCATED not in defects:
            handle.write(DELIMITER.join(trailer) + "\n")
    return target


def _iso_local_time(value: str) -> str:
    return f"{datetime.strptime(value, '%Y%m%d%H%M%S'):%Y-%m-%d %H:%M:%S}"


def read_file(path: Path) -> CardFile:
    """Parse a processor file. A missing trailer is returned as ``T|-1|-1``, which never matches."""
    header: tuple[str, ...] | None = None
    trailer: tuple[str, ...] = ("T", "-1", "-1")
    details: list[tuple[str, ...]] = []
    with path.open(encoding="ascii", newline="") as handle:
        for line in handle:
            record = tuple(line.rstrip("\n").split(DELIMITER))
            if record[0] == "H":
                header = record
            elif record[0] == "D":
                details.append(record)
            elif record[0] == "T":
                trailer = record
            else:
                raise ValueError(f"{path.name}: unknown record type {record[0]!r}")
    if header is None:
        raise ValueError(f"{path.name}: header missing")
    return CardFile(header, details, trailer)


def business_dates(start: date, end: date) -> Iterator[date]:
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)
