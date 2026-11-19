"""Driver-side checks on landed files, done before Spark reads a single record.

Everything here works on the file as a whole (name, checksum, first and last
line), which is cheap on the driver and decides whether Spark should read the
file at all.
"""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path

from aml_platform.sources.landing import Manifest, sha256_of

PAYMENTS_FILE = re.compile(r"PAYMENTS_(\d{8})\.csv")
CARDS_FILE = re.compile(r"CARDTXN_(\d{8})_(\d{3})\.dat")
CARD_FORMAT_VERSION = "01"
CARD_DELIMITER = "|"


class FileRejectedError(Exception):
    """The file breaks its contract and must not be ingested."""

    def __init__(self, reason: str, *, business_date: date | None = None, records_in_file: int | None = None) -> None:
        super().__init__(reason)
        self.business_date = business_date
        self.records_in_file = records_in_file


@dataclass(frozen=True)
class LandedFile:
    path: Path
    sha256: str

    @property
    def name(self) -> str:
        return self.path.name


@dataclass(frozen=True)
class CardControl:
    business_date: date
    sequence: str
    declared_count: int
    declared_total: int


def _yyyymmdd(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def landed(path: Path) -> LandedFile:
    return LandedFile(path, sha256_of(path))


def data_files(directory: Path, pattern: re.Pattern[str], business_date: date | None = None) -> list[Path]:
    """Delivered files in name order; partial and hidden files are still being written.

    With ``business_date``, only the files of that date: the first group of ``pattern``
    must then capture the date as YYYYMMDD.
    """
    if not directory.is_dir():
        return []
    wanted = None if business_date is None else f"{business_date:%Y%m%d}"
    selected = []
    for path in directory.iterdir():
        match = pattern.fullmatch(path.name)
        if match and path.is_file() and (wanted is None or match.group(1) == wanted):
            selected.append(path)
    return sorted(selected)


def payments_business_date(path: Path) -> date:
    match = PAYMENTS_FILE.fullmatch(path.name)
    if match is None:
        raise FileRejectedError(f"unexpected file name {path.name}")
    return _yyyymmdd(match.group(1))


def verified_manifest(file: LandedFile) -> Manifest:
    """The manifest of ``file``, checked against the file. Raises FileNotFoundError while it is not delivered."""
    manifest = Manifest.read(Manifest.path_for(file.path))
    if manifest.file_name != file.name:
        raise FileRejectedError(f"manifest describes {manifest.file_name}, not {file.name}")
    if manifest.sha256 != file.sha256:
        raise FileRejectedError(f"sha256 {file.sha256} differs from manifest {manifest.sha256}")
    return manifest


def first_line(path: Path) -> str:
    with path.open("rb") as handle:
        return handle.readline().decode("ascii", errors="replace").rstrip("\r\n")


def last_line(path: Path, *, window: int = 4096) -> str:
    with path.open("rb") as handle:
        size = handle.seek(0, 2)
        handle.seek(max(0, size - window))
        tail = handle.read().rstrip(b"\r\n")
    return tail.rsplit(b"\n", 1)[-1].decode("ascii", errors="replace")


def check_csv_header(path: Path, columns: tuple[str, ...]) -> None:
    header = first_line(path)
    if header != ",".join(columns):
        raise FileRejectedError(f"header {header!r} does not match the contract columns")


def card_control(path: Path) -> CardControl:
    """Validate the header against the file name and return the trailer's control totals."""
    match = CARDS_FILE.fullmatch(path.name)
    if match is None:
        raise FileRejectedError(f"unexpected file name {path.name}")
    header = first_line(path).split(CARD_DELIMITER)
    if len(header) != 7 or header[:2] != ["H", "CARDTXN"]:
        raise FileRejectedError("first record is not a CARDTXN header")
    if header[2] != CARD_FORMAT_VERSION:
        raise FileRejectedError(f"unsupported format version {header[2]!r}")
    if (header[4], header[5]) != match.groups():
        raise FileRejectedError(f"header date/sequence {header[4]}/{header[5]} disagree with the file name")
    trailer = last_line(path).split(CARD_DELIMITER)
    if trailer[0] != "T":
        raise FileRejectedError("trailer missing: the transfer was cut short")
    if len(trailer) != 3 or not (trailer[1].isdigit() and trailer[2].isdigit()):
        raise FileRejectedError(f"malformed trailer {CARD_DELIMITER.join(trailer)!r}")
    return CardControl(_yyyymmdd(header[4]), header[5], int(trailer[1]), int(trailer[2]))


@dataclass(frozen=True)
class Rejection:
    file_name: str
    sha256: str
    source: str
    contract_id: str
    run_id: str
    reason: str
    rejected_at: str


def quarantine(file: LandedFile, rejection: Rejection, quarantine_dir: Path) -> Path:
    """Keep a copy of the rejected file and the reason next to it.

    The landing zone is read-only to ingestion: the original stays where the
    sender put it, and a corrected redelivery is recognised by its new checksum.
    """
    target_dir = quarantine_dir / rejection.source
    target_dir.mkdir(parents=True, exist_ok=True)
    copy = target_dir / f"{file.sha256[:12]}_{file.name}"
    if not copy.exists():
        shutil.copyfile(file.path, copy)
    report = copy.with_name(f"{copy.name}.rejection.json")
    report.write_text(json.dumps(asdict(rejection), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return copy
