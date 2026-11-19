"""Payments hub feed: one CSV per business date, built from the IBM transaction file.

The laundering label is split off into a ground-truth file that the platform never
ingests. Detection rules therefore cannot see it, and it is used only to measure
their precision and recall.

Bank identifiers are passed through exactly as the hub emits them (zero-padded,
with inconsistent widths for the same bank); normalising them is the job of the
silver layer, not of the source.
"""

from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from collections.abc import Iterator
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any

from aml_platform.sources.landing import Manifest, atomic_writer, sha256_of

FEED_COLUMNS: tuple[str, ...] = (
    "transfer_id",
    "booked_at",
    "originator_bank_id",
    "originator_account",
    "beneficiary_bank_id",
    "beneficiary_account",
    "amount_paid",
    "payment_currency",
    "amount_received",
    "receiving_currency",
    "payment_format",
)
GROUND_TRUTH_COLUMNS: tuple[str, ...] = ("transfer_id", "attempt_id", "typology")

IBM_HEADER: tuple[str, ...] = (
    "Timestamp",
    "From Bank",
    "Account",
    "To Bank",
    "Account",
    "Amount Received",
    "Receiving Currency",
    "Amount Paid",
    "Payment Currency",
    "Payment Format",
    "Is Laundering",
)

CURRENCIES: dict[str, str] = {
    "Australian Dollar": "AUD",
    "Bitcoin": "XBT",
    "Brazil Real": "BRL",
    "Canadian Dollar": "CAD",
    "Euro": "EUR",
    "Mexican Peso": "MXN",
    "Ruble": "RUB",
    "Rupee": "INR",
    "Saudi Riyal": "SAR",
    "Shekel": "ILS",
    "Swiss Franc": "CHF",
    "UK Pound": "GBP",
    "US Dollar": "USD",
    "Yen": "JPY",
    "Yuan": "CNY",
}
PAYMENT_FORMATS: dict[str, str] = {
    "ACH": "ACH",
    "Bitcoin": "BITCOIN",
    "Cash": "CASH",
    "Cheque": "CHEQUE",
    "Credit Card": "CREDIT_CARD",
    "Reinvestment": "REINVESTMENT",
    "Wire": "WIRE",
}

_TIMESTAMP = re.compile(r"(\d{4})/(\d{2})/(\d{2}) (\d{2}):(\d{2})")
_AMOUNT = re.compile(r"\d{1,16}(?:\.\d{1,6})?")
_PATTERN_BEGIN = re.compile(r"BEGIN LAUNDERING ATTEMPT - ([A-Z-]+)")


class SourceFormatError(ValueError):
    pass


@dataclass(frozen=True)
class FeedSummary:
    files: tuple[Path, ...]
    record_count: int
    laundering_count: int
    unmatched_pattern_rows: int


def feed_file_name(business_date: str) -> str:
    return f"PAYMENTS_{business_date.replace('-', '')}.csv"


def transfer_id(line_number: int) -> str:
    return f"TRF{line_number:09d}"


def _convert(row: list[str], line_number: int) -> tuple[str, list[str]]:
    if len(row) != len(IBM_HEADER):
        raise SourceFormatError(f"line {line_number}: expected {len(IBM_HEADER)} fields, got {len(row)}")
    timestamp, from_bank, from_account, to_bank, to_account, received, rcv_ccy, paid, pay_ccy, fmt, _ = row
    match = _TIMESTAMP.fullmatch(timestamp)
    if match is None:
        raise SourceFormatError(f"line {line_number}: bad timestamp {timestamp!r}")
    year, month, day, hour, minute = match.groups()
    for amount in (received, paid):
        if _AMOUNT.fullmatch(amount) is None:
            raise SourceFormatError(f"line {line_number}: bad amount {amount!r}")
    try:
        payment_currency, receiving_currency = CURRENCIES[pay_ccy], CURRENCIES[rcv_ccy]
        payment_format = PAYMENT_FORMATS[fmt]
    except KeyError as unknown:
        raise SourceFormatError(f"line {line_number}: unmapped code {unknown}") from None
    business_date = f"{year}-{month}-{day}"
    return business_date, [
        transfer_id(line_number),
        f"{business_date}T{hour}:{minute}:00",
        from_bank,
        from_account,
        to_bank,
        to_account,
        paid,
        payment_currency,
        received,
        receiving_currency,
        payment_format,
    ]


def parse_patterns(path: Path) -> Iterator[tuple[int, str, str]]:
    """Yield ``(attempt_id, typology, raw transaction line)`` from the IBM patterns file."""
    attempt_id, typology = 0, ""
    with path.open(encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            if begin := _PATTERN_BEGIN.match(line):
                attempt_id += 1
                typology = begin.group(1)
            elif line.startswith("END LAUNDERING ATTEMPT"):
                typology = ""
            elif typology:
                yield attempt_id, typology, line


def build_feed(transactions: Path, patterns: Path, landing_dir: Path, ground_truth_dir: Path) -> FeedSummary:
    pending: dict[str, list[tuple[int, str]]] = defaultdict(list)
    for attempt_id, typology, line in parse_patterns(patterns):
        pending[line].append((attempt_id, typology))

    counts: Counter[str] = Counter()
    laundering = 0
    with ExitStack() as stack:
        writers: dict[str, Any] = {}

        def writer_for(business_date: str) -> Any:
            if business_date not in writers:
                handle: IO[str] = stack.enter_context(
                    atomic_writer(landing_dir / feed_file_name(business_date), encoding="utf-8", newline="")
                )
                writers[business_date] = csv.writer(handle, lineterminator="\n")
                writers[business_date].writerow(FEED_COLUMNS)
            return writers[business_date]

        truth_handle = stack.enter_context(
            atomic_writer(ground_truth_dir / "laundering_transfers.csv", encoding="utf-8", newline="")
        )
        truth = csv.writer(truth_handle, lineterminator="\n")
        truth.writerow(GROUND_TRUTH_COLUMNS)

        with transactions.open(encoding="utf-8", newline="") as source:
            header = source.readline().rstrip("\r\n")
            if tuple(header.split(",")) != IBM_HEADER:
                raise SourceFormatError(f"unexpected header: {header!r}")
            for line_number, raw in enumerate(source, start=1):
                line = raw.rstrip("\r\n")
                row = line.split(",")
                business_date, record = _convert(row, line_number)
                writer_for(business_date).writerow(record)
                counts[business_date] += 1
                if row[-1] == "1":
                    laundering += 1
                    matches = pending.get(line)
                    attempt: tuple[int | str, str] = matches.pop(0) if matches else ("", "")
                    truth.writerow((record[0], *attempt))
                elif row[-1] != "0":
                    raise SourceFormatError(f"line {line_number}: bad label {row[-1]!r}")

    files: list[Path] = []
    for business_date in sorted(counts):
        data_file = landing_dir / feed_file_name(business_date)
        Manifest(
            file_name=data_file.name,
            record_count=counts[business_date],
            sha256=sha256_of(data_file),
            attributes={"business_date": business_date, "feed": "payments"},
        ).write_next_to(data_file)
        files.append(data_file)
    return FeedSummary(
        files=tuple(files),
        record_count=sum(counts.values()),
        laundering_count=laundering,
        unmatched_pattern_rows=sum(len(rows) for rows in pending.values()),
    )
