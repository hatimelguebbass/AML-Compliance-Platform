"""Core banking customer master: customers, accounts and cards in PostgreSQL.

Every account holder of the IBM account list becomes a customer, so each account
in the payments feed resolves to a KYC record. Two kinds of known defects are
planted, with their answers kept in ground-truth files outside the database:

* duplicate customer records for the same party (name transliteration, ID
  formatting, swapped day and month), which the customer 360 must resolve;
* customers whose names match individuals on the UN or OFAC lists, which
  sanctions screening must find.

Generation is deterministic per party: the same seed and inputs always produce
the same rows, whatever the processing order.
"""

from __future__ import annotations

import csv
import logging
import random
from collections.abc import Iterable, Sequence
from dataclasses import astuple, dataclass, fields
from datetime import UTC, date, datetime, time, timedelta
from importlib.resources import files
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from aml_platform.sources import reference as ref
from aml_platform.sources.landing import atomic_writer
from aml_platform.sources.payments import CURRENCIES, IBM_HEADER
from aml_platform.sources.sanctions import ListedPerson

log = logging.getLogger(__name__)

BANK_CODE = "990"
CARD_PRODUCTS: tuple[tuple[str, float], ...] = (("DEBIT_CLASSIC", 0.80), ("DEBIT_GOLD", 0.15), ("DEBIT_PLATINUM", 0.05))
CARD_SCHEMES: tuple[str, ...] = ("VISA", "MASTERCARD", "CMI")
# Last day before the first business date of the payments feed.
AS_OF = date(2022, 8, 31)

PERSON_ENTITY_TYPES = frozenset({"Individual", "Sole Proprietorship"})
LEGAL_FORMS: dict[str, tuple[tuple[str, float], ...]] = {
    "Corporation": (("SARL", 0.55), ("SA", 0.25), ("SARL_AU", 0.20)),
    "Partnership": (("SNC", 0.70), ("SCS", 0.30)),
    "Country": (("EP", 1.0),),
    "Direct": (("EP", 1.0),),
}


@dataclass(frozen=True)
class Customer:
    customer_id: int
    customer_type: str
    first_name: str | None
    last_name: str | None
    gender: str | None
    birth_date: date | None
    legal_name: str | None
    legal_form: str | None
    incorporation_date: date | None
    id_document_type: str | None
    id_document_number: str | None
    tax_id: str | None
    nationality: str | None
    residence_country: str
    occupation: str | None
    address_line: str
    city: str
    postal_code: str
    phone: str | None
    email: str | None
    risk_rating: str
    is_pep: bool
    customer_since: date
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class Account:
    account_id: int
    customer_id: int
    bank_id: str
    account_number: str
    rib: str
    account_type: str
    currency: str
    status: str
    opened_on: date
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class Card:
    card_id: int
    account_id: int
    card_token: str
    scheme: str
    pan_last4: str
    product: str
    status: str
    issued_on: date
    expires_on: date
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class IbmAccount:
    bank_id: str
    account_number: str
    entity_id: str
    entity_type: str


@dataclass(frozen=True)
class GenerationConfig:
    seed: int = 20220901
    duplicate_rate_person: float = 0.04
    duplicate_rate_organization: float = 0.02
    card_rate: float = 0.85
    pep_rate: float = 0.002
    sanctioned_customers: int = 40


@dataclass
class CoreBankingData:
    customers: list[Customer]
    accounts: list[Account]
    cards: list[Card]
    parties: list[tuple[int, str, str]]  # customer_id, party_id, record role
    sanctioned: list[tuple[int, str, str, str]]  # customer_id, list, entry id, listed name


def read_ibm_accounts(path: Path) -> list[IbmAccount]:
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        expected = ["Bank Name", "Bank ID", "Account Number", "Entity ID", "Entity Name"]
        if reader.fieldnames != expected:
            raise ValueError(f"unexpected header {reader.fieldnames}")
        return [
            IbmAccount(row["Bank ID"], row["Account Number"], row["Entity ID"], row["Entity Name"].split(" #")[0])
            for row in reader
        ]


def normalize_bank_id(bank_id: str) -> str:
    return bank_id.lstrip("0") or "0"


def account_currencies(transactions: Path) -> dict[tuple[str, str], str]:
    """Currency of each account: the first one it pays in, else the first one it receives in."""
    paid: dict[tuple[str, str], str] = {}
    received: dict[tuple[str, str], str] = {}
    with transactions.open(encoding="utf-8", newline="") as handle:
        if tuple(handle.readline().rstrip("\r\n").split(",")) != IBM_HEADER:
            raise ValueError("unexpected transactions header")
        for line in handle:
            row = line.rstrip("\r\n").split(",")
            paid.setdefault((normalize_bank_id(row[1]), row[2]), row[8])
            received.setdefault((normalize_bank_id(row[3]), row[4]), row[6])
    return {key: CURRENCIES[name] for key, name in (received | paid).items()}


def rib(locality: str, account_id: int) -> str:
    body = f"{BANK_CODE}{locality}{account_id:016d}"
    key = 97 - (int(body) * 100) % 97
    return f"{body}{key:02d}"


def _weighted(rng: random.Random, options: Sequence[tuple[str, float]]) -> str:
    return rng.choices([value for value, _ in options], weights=[weight for _, weight in options])[0]


def _days_before(rng: random.Random, latest: date, min_days: int, max_days: int) -> date:
    return latest - timedelta(days=rng.randint(min_days, max_days))


def _timestamp(rng: random.Random, day: date) -> datetime:
    return datetime.combine(day, time(rng.randint(8, 17), rng.randint(0, 59), rng.randint(0, 59)), tzinfo=UTC)


def _risk(rng: random.Random, is_pep: bool) -> str:
    if is_pep:
        return "HIGH"
    return _weighted(rng, (("LOW", 0.82), ("MEDIUM", 0.15), ("HIGH", 0.03)))


def _phone(rng: random.Random) -> str:
    return f"+212{rng.choice('67')}{rng.randint(0, 99_999_999):08d}"


def _new_person(rng: random.Random, customer_id: int, entity_type: str, pep_rate: float) -> Customer:
    gender = rng.choice("FM")
    first = rng.choice(ref.FEMALE_FIRST_NAMES if gender == "F" else ref.MALE_FIRST_NAMES)
    last = rng.choice(ref.LAST_NAMES)
    city, cin_prefix, postal_code, _ = rng.choice(ref.CITIES)
    foreign_national = rng.random() < 0.04
    nationality = rng.choice(ref.NATIONALITIES_ABROAD) if foreign_national else "MA"
    residence = rng.choice(ref.RESIDENCE_ABROAD) if not foreign_national and rng.random() < 0.08 else "MA"
    if foreign_national:
        document_type, document = "PASSPORT", f"{rng.choice('ABCDEFGHJKLMNPRSTUVWXYZ')}{rng.randint(0, 9_999_999):08d}"
    else:
        document_type, document = "CIN", f"{cin_prefix}{rng.randint(1_000, 999_999)}"
    since = _days_before(rng, AS_OF, 0, 8_000)
    occupation = (
        rng.choice(("SELF_EMPLOYED", "MERCHANT", "CRAFTSMAN", "LIBERAL_PROFESSION"))
        if entity_type == "Sole Proprietorship"
        else rng.choice(ref.OCCUPATIONS)
    )
    is_pep = rng.random() < pep_rate
    created = _timestamp(rng, since)
    email = None
    if rng.random() < 0.6:
        handle = f"{first}.{last}".lower().replace(" ", "")
        email = f"{handle}{rng.randint(1, 99)}@{rng.choice(('gmail.com', 'outlook.com', 'yahoo.fr', 'menara.ma'))}"
    return Customer(
        customer_id=customer_id,
        customer_type="PERSON",
        first_name=first,
        last_name=last,
        gender=gender,
        birth_date=_days_before(rng, AS_OF, 18 * 366, 85 * 365),
        legal_name=None,
        legal_form=None,
        incorporation_date=None,
        id_document_type=document_type,
        id_document_number=document,
        tax_id=None,
        nationality=nationality,
        residence_country=residence,
        occupation=occupation,
        address_line=f"{rng.randint(1, 250)} {rng.choice(ref.STREETS)}",
        city=city,
        postal_code=postal_code,
        phone=_phone(rng),
        email=email,
        risk_rating=_risk(rng, is_pep),
        is_pep=is_pep,
        customer_since=since,
        created_at=created,
        updated_at=created,
    )


def _new_organization(rng: random.Random, customer_id: int, entity_type: str) -> Customer:
    legal_form = _weighted(rng, LEGAL_FORMS.get(entity_type, LEGAL_FORMS["Corporation"]))
    city, _, postal_code, _ = rng.choice(ref.CITIES)
    name = f"{rng.choice(ref.COMPANY_WORDS)} {rng.choice(ref.COMPANY_ACTIVITIES)}"
    incorporated = _days_before(rng, AS_OF, 200, 12_000)
    since = _days_before(rng, AS_OF, 0, (AS_OF - incorporated).days)
    created = _timestamp(rng, since)
    return Customer(
        customer_id=customer_id,
        customer_type="ORGANIZATION",
        first_name=None,
        last_name=None,
        gender=None,
        birth_date=None,
        legal_name=f"{name} {legal_form.replace('_', ' ')}",
        legal_form=legal_form,
        incorporation_date=incorporated,
        id_document_type="RC",
        id_document_number=f"{rng.randint(1_000, 699_999)}",
        tax_id=f"00{rng.randint(0, 10**13 - 1):013d}",
        nationality=None,
        residence_country="MA",
        occupation=None,
        address_line=f"{rng.randint(1, 250)} {rng.choice(ref.STREETS)}",
        city=city,
        postal_code=postal_code,
        phone=_phone(rng),
        email=None,
        risk_rating=_risk(rng, False),
        is_pep=False,
        customer_since=since,
        created_at=created,
        updated_at=created,
    )


def _variant(rng: random.Random, name: str) -> str:
    if name in ref.NAME_VARIANTS and rng.random() < 0.7:
        return rng.choice(ref.NAME_VARIANTS[name])
    return rng.choice((name.upper(), name.lower(), name.replace(" ", "")))


def _duplicate_of(rng: random.Random, original: Customer, customer_id: int) -> Customer:
    """A second record for the same party, as created when a branch skips the customer search."""
    since = original.customer_since + timedelta(days=rng.randint(30, 2_000))
    since = min(since, AS_OF)
    created = _timestamp(rng, since)
    changes: dict[str, Any] = {
        "customer_id": customer_id,
        "customer_since": since,
        "created_at": created,
        "updated_at": created,
        "phone": _phone(rng) if rng.random() < 0.5 else original.phone,
        "email": None,
    }
    document = original.id_document_number
    if document is not None:
        roll = rng.random()
        if roll < 0.2:
            changes["id_document_type"], changes["id_document_number"] = None, None
        elif roll < 0.7:
            prefix = document.rstrip("0123456789")
            changes["id_document_number"] = rng.choice(
                (f"{prefix} {document[len(prefix) :]}", document.lower(), f"{prefix}-{document[len(prefix) :]}")
            )
    if original.first_name and original.last_name and original.birth_date:
        if rng.random() < 0.6:
            changes["first_name"] = _variant(rng, original.first_name)
        if rng.random() < 0.6:
            changes["last_name"] = _variant(rng, original.last_name)
        birth = original.birth_date
        if birth.day <= 12 and birth.day != birth.month and rng.random() < 0.15:
            changes["birth_date"] = birth.replace(day=birth.month, month=birth.day)
    elif original.legal_name and original.legal_form:
        base = original.legal_name.removesuffix(original.legal_form.replace("_", " ")).strip()
        changes["legal_name"] = rng.choice((original.legal_name.upper(), f"STE {base.upper()}", base))
    if rng.random() < 0.5:
        changes["address_line"] = f"{rng.randint(1, 250)} {rng.choice(ref.STREETS)}"
    values = {field.name: getattr(original, field.name) for field in fields(original)}
    return Customer(**(values | changes))


def _plant_sanction(rng: random.Random, customer: Customer, person: ListedPerson) -> Customer:
    names = list(person.names)
    if len(names) > 2 and rng.random() < 0.3:
        del names[rng.randrange(1, len(names) - 1)]
    names = [name.title() if rng.random() < 0.7 else name for name in names]
    values = {field.name: getattr(customer, field.name) for field in fields(customer)}
    return Customer(**(values | {"first_name": names[0], "last_name": " ".join(names[1:])}))


def _eligible_for_plant(person: ListedPerson) -> bool:
    return 2 <= len(person.names) <= 4 and all(
        name.replace("-", "").replace("'", "").isalpha() for name in person.names
    )


def generate(
    ibm_accounts: Sequence[IbmAccount],
    currencies: dict[tuple[str, str], str],
    listed: Sequence[ListedPerson],
    config: GenerationConfig,
) -> CoreBankingData:
    by_party: dict[str, list[IbmAccount]] = {}
    for holding in ibm_accounts:
        by_party.setdefault(holding.entity_id, []).append(holding)

    customers: list[Customer] = []
    accounts: list[Account] = []
    cards: list[Card] = []
    parties: list[tuple[int, str, str]] = []
    duplicates: list[tuple[Customer, str, list[IbmAccount], random.Random]] = []
    customer_id = 1_000_000

    for party_id in sorted(by_party):
        rng = random.Random(f"{config.seed}|{party_id}")
        holdings = sorted(by_party[party_id], key=lambda a: (normalize_bank_id(a.bank_id), a.account_number))
        entity_type = holdings[0].entity_type
        customer_id += 1
        is_person = entity_type in PERSON_ENTITY_TYPES
        customer = (
            _new_person(rng, customer_id, entity_type, config.pep_rate)
            if is_person
            else _new_organization(rng, customer_id, entity_type)
        )
        customers.append(customer)
        parties.append((customer.customer_id, party_id, "primary"))
        rate = config.duplicate_rate_person if is_person else config.duplicate_rate_organization
        if len(holdings) > 1 and rng.random() < rate:
            moved = rng.randint(1, len(holdings) - 1)
            duplicates.append((customer, party_id, holdings[-moved:], rng))
            holdings = holdings[:-moved]
        accounts.extend(_accounts_for(rng, customer, holdings, currencies, len(accounts)))

    for original, party_id, holdings, rng in duplicates:
        customer_id += 1
        duplicate = _duplicate_of(rng, original, customer_id)
        customers.append(duplicate)
        parties.append((duplicate.customer_id, party_id, "duplicate"))
        accounts.extend(_accounts_for(rng, duplicate, holdings, currencies, len(accounts)))

    owners = {customer.customer_id: customer for customer in customers}
    card_rng = random.Random(f"{config.seed}|cards")
    tokens: set[str] = set()
    for account in accounts:
        owner = owners[account.customer_id]
        if (
            owner.customer_type != "PERSON"
            or account.account_type != "CURRENT"
            or card_rng.random() >= config.card_rate
        ):
            continue
        token = f"9{card_rng.randint(0, 10**15 - 1):015d}"
        while token in tokens:
            token = f"9{card_rng.randint(0, 10**15 - 1):015d}"
        tokens.add(token)
        issued = max(account.opened_on, AS_OF - timedelta(days=card_rng.randint(0, 4 * 365)))
        expiry_month = date(issued.year + 4, issued.month, 1)
        expires = (expiry_month.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        created = _timestamp(card_rng, issued)
        cards.append(
            Card(
                card_id=len(cards) + 1,
                account_id=account.account_id,
                card_token=token,
                scheme=card_rng.choice(CARD_SCHEMES),
                pan_last4=f"{card_rng.randint(0, 9999):04d}",
                product=_weighted(card_rng, CARD_PRODUCTS),
                status="BLOCKED" if card_rng.random() < 0.01 else "ACTIVE",
                issued_on=issued,
                expires_on=expires,
                created_at=created,
                updated_at=created,
            )
        )

    sanctioned = _plant_sanctions(customers, parties, listed, config)
    return CoreBankingData(customers, accounts, cards, parties, sanctioned)


def _accounts_for(
    rng: random.Random,
    customer: Customer,
    holdings: Iterable[IbmAccount],
    currencies: dict[tuple[str, str], str],
    offset: int,
) -> list[Account]:
    locality = next(c[3] for c in ref.CITIES if c[0] == customer.city)
    result: list[Account] = []
    for index, holding in enumerate(holdings):
        account_id = offset + index + 1
        bank_id = normalize_bank_id(holding.bank_id)
        currency = currencies.get((bank_id, holding.account_number))
        opened = _days_before(rng, AS_OF, 0, (AS_OF - customer.customer_since).days)
        created = _timestamp(rng, opened)
        account_type = (
            "CURRENT" if index == 0 or customer.customer_type != "PERSON" else rng.choice(("CURRENT", "SAVINGS"))
        )
        result.append(
            Account(
                account_id=account_id,
                customer_id=customer.customer_id,
                bank_id=bank_id,
                account_number=holding.account_number,
                rib=rib(locality, account_id),
                account_type=account_type,
                currency=currency or "MAD",
                status="ACTIVE" if currency else "DORMANT",
                opened_on=opened,
                created_at=created,
                updated_at=created,
            )
        )
    return result


def _plant_sanctions(
    customers: list[Customer],
    parties: list[tuple[int, str, str]],
    listed: Sequence[ListedPerson],
    config: GenerationConfig,
) -> list[tuple[int, str, str, str]]:
    if config.sanctioned_customers == 0:
        return []
    candidates = sorted((p for p in listed if _eligible_for_plant(p)), key=lambda p: (p.list_name, p.entry_id))
    if len(candidates) < config.sanctioned_customers:
        raise ValueError(f"only {len(candidates)} listed persons usable, {config.sanctioned_customers} requested")
    rng = random.Random(f"{config.seed}|sanctions")
    # Parties with a duplicate record are left out so that every record of a planted party carries the listed name.
    duplicated = {party for _, party, role in parties if role == "duplicate"}
    single = {cid for cid, party, _ in parties if party not in duplicated}
    positions = [i for i, c in enumerate(customers) if c.customer_type == "PERSON" and c.customer_id in single]
    planted: list[tuple[int, str, str, str]] = []
    for position, person in zip(
        rng.sample(positions, config.sanctioned_customers),
        rng.sample(candidates, config.sanctioned_customers),
        strict=True,
    ):
        customer = _plant_sanction(rng, customers[position], person)
        customers[position] = customer
        planted.append((customer.customer_id, person.list_name, person.entry_id, " ".join(person.names)))
    return planted


def write_ground_truth(data: CoreBankingData, ground_truth_dir: Path) -> None:
    for name, header, rows in (
        ("customer_parties.csv", ("customer_id", "party_id", "record_role"), data.parties),
        ("sanctioned_customers.csv", ("customer_id", "list", "list_entry_id", "listed_name"), data.sanctioned),
    ):
        with atomic_writer(ground_truth_dir / name, encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(header)
            writer.writerows(rows)


def schema_ddl() -> str:
    return files("aml_platform.sources").joinpath("sql/corebanking.sql").read_text(encoding="utf-8")


class TargetNotEmptyError(RuntimeError):
    pass


def load(conninfo: str, data: CoreBankingData, *, schema: str = "core", replace: bool = False) -> None:
    """Create the core banking schema if needed and bulk-load the generated rows in one transaction."""
    tables: tuple[tuple[str, type, list[Any]], ...] = (
        ("customer", Customer, data.customers),
        ("account", Account, data.accounts),
        ("card", Card, data.cards),
    )
    with psycopg.connect(conninfo) as connection, connection.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        cursor.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(schema)))
        cursor.execute(schema_ddl().encode())
        cursor.execute("SELECT EXISTS (SELECT 1 FROM customer)")
        row = cursor.fetchone()
        if row is not None and row[0]:
            if not replace:
                raise TargetNotEmptyError(f"{schema}.customer already holds data; pass --replace to reload")
            cursor.execute("TRUNCATE card, account, customer")
        for table, row_type, rows in tables:
            columns = sql.SQL(", ").join(sql.Identifier(f.name) for f in fields(row_type))
            statement = sql.SQL("COPY {} ({}) FROM STDIN").format(sql.Identifier(table), columns)
            with cursor.copy(statement) as copy:
                for record in rows:
                    copy.write_row(astuple(record))
            log.info("loaded %s rows into %s.%s", len(rows), schema, table)
