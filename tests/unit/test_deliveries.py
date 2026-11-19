from __future__ import annotations

from datetime import date
from pathlib import Path

from aml_platform.orchestration.deliveries import missing_deliveries
from aml_platform.sources.landing import Manifest, sha256_of

DAY = date(2022, 9, 5)


def _payments(landing: Path, *, manifest: bool) -> None:
    directory = landing / "payments"
    directory.mkdir(parents=True, exist_ok=True)
    data = directory / "PAYMENTS_20220905.csv"
    data.write_text("transfer_id\nTRF000000001\n", encoding="utf-8")
    if manifest:
        Manifest(data.name, 1, sha256_of(data), {}).write_next_to(data)


def _cards(landing: Path, name: str = "CARDTXN_20220905_001.dat") -> None:
    directory = landing / "cards"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).touch()


def test_complete_day_has_nothing_missing(tmp_path: Path) -> None:
    _payments(tmp_path, manifest=True)
    _cards(tmp_path)
    assert missing_deliveries(tmp_path, DAY) == []


def test_empty_landing_zone_misses_both_feeds(tmp_path: Path) -> None:
    assert missing_deliveries(tmp_path, DAY) == [
        "payments file for 2022-09-05",
        "card processor file for 2022-09-05",
    ]


def test_payments_without_manifest_are_still_being_delivered(tmp_path: Path) -> None:
    _payments(tmp_path, manifest=False)
    _cards(tmp_path)
    assert missing_deliveries(tmp_path, DAY) == ["manifest of PAYMENTS_20220905.csv"]


def test_files_of_other_dates_do_not_count(tmp_path: Path) -> None:
    _payments(tmp_path, manifest=True)
    _cards(tmp_path, "CARDTXN_20220906_001.dat")
    assert missing_deliveries(tmp_path, DAY) == ["card processor file for 2022-09-05"]
