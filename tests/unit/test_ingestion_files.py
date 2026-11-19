from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from aml_platform.ingestion import files
from aml_platform.sources import cards
from aml_platform.sources.landing import Manifest, sha256_of

DAY = date(2022, 9, 5)
HOLDERS = [cards.CardHolder(f"9{i:015d}", city) for i, city in enumerate(["Rabat", "Fes"] * 10)]


def _card_file(directory: Path, *defects: cards.Defect) -> Path:
    merchants = cards.build_merchants([h.home_city for h in HOLDERS], seed=1, per_city=20)
    return cards.generate_file(directory, DAY, HOLDERS, merchants, count=50, seed=1, defects=frozenset(defects))


def _delivered(directory: Path, name: str = "PAYMENTS_20220901.csv") -> Path:
    data = directory / name
    data.write_text("transfer_id\nTRF000000001\n", encoding="utf-8")
    Manifest(data.name, 1, sha256_of(data), {}).write_next_to(data)
    return data


def test_partial_and_foreign_files_are_not_listed(tmp_path: Path) -> None:
    for name in ("PAYMENTS_20220902.csv", "PAYMENTS_20220901.csv", ".PAYMENTS_20220903.csv.partial", "notes.txt"):
        (tmp_path / name).touch()
    listed = files.data_files(tmp_path, files.PAYMENTS_FILE)
    assert [p.name for p in listed] == ["PAYMENTS_20220901.csv", "PAYMENTS_20220902.csv"]
    assert files.data_files(tmp_path / "missing", files.PAYMENTS_FILE) == []


def test_a_business_date_selects_only_its_files(tmp_path: Path) -> None:
    for name in ("CARDTXN_20220905_001.dat", "CARDTXN_20220905_002.dat", "CARDTXN_20220906_001.dat"):
        (tmp_path / name).touch()
    listed = files.data_files(tmp_path, files.CARDS_FILE, DAY)
    assert [p.name for p in listed] == ["CARDTXN_20220905_001.dat", "CARDTXN_20220905_002.dat"]
    assert files.data_files(tmp_path, files.CARDS_FILE, date(2022, 9, 7)) == []


def test_manifest_agreeing_with_file_is_returned(tmp_path: Path) -> None:
    data = _delivered(tmp_path)
    assert files.verified_manifest(files.landed(data)).record_count == 1


def test_file_changed_after_its_manifest_is_rejected(tmp_path: Path) -> None:
    data = _delivered(tmp_path)
    data.write_text("transfer_id\nTRF000000002\n", encoding="utf-8")
    with pytest.raises(files.FileRejectedError, match="sha256"):
        files.verified_manifest(files.landed(data))


def test_manifest_of_another_file_is_rejected(tmp_path: Path) -> None:
    data = _delivered(tmp_path)
    renamed = data.rename(tmp_path / "PAYMENTS_20220902.csv")
    Manifest.path_for(data).rename(Manifest.path_for(renamed))
    with pytest.raises(files.FileRejectedError, match="manifest describes"):
        files.verified_manifest(files.landed(renamed))


def test_missing_manifest_means_delivery_in_progress(tmp_path: Path) -> None:
    data = tmp_path / "PAYMENTS_20220901.csv"
    data.write_text("transfer_id\n", encoding="utf-8")
    with pytest.raises(FileNotFoundError):
        files.verified_manifest(files.landed(data))


def test_payments_business_date_comes_from_the_name() -> None:
    assert files.payments_business_date(Path("PAYMENTS_20220918.csv")) == date(2022, 9, 18)


def test_csv_header_must_match_contract_columns(tmp_path: Path) -> None:
    data = _delivered(tmp_path)
    files.check_csv_header(data, ("transfer_id",))
    with pytest.raises(files.FileRejectedError, match="header"):
        files.check_csv_header(data, ("transfer_id", "booked_at"))


def test_complete_card_file_yields_its_trailer_totals(tmp_path: Path) -> None:
    path = _card_file(tmp_path)
    parsed = cards.read_file(path)
    control = files.card_control(path)
    assert control == files.CardControl(DAY, "001", len(parsed.details), int(parsed.trailer[2]))


def test_truncated_card_file_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(files.FileRejectedError, match="trailer missing"):
        files.card_control(_card_file(tmp_path, cards.Defect.TRUNCATED))


def test_card_header_must_match_file_name(tmp_path: Path) -> None:
    path = _card_file(tmp_path).rename(tmp_path / "CARDTXN_20220906_001.dat")
    with pytest.raises(files.FileRejectedError, match="disagree with the file name"):
        files.card_control(path)


def test_unknown_card_format_version_is_rejected(tmp_path: Path) -> None:
    path = _card_file(tmp_path)
    path.write_text(path.read_text(encoding="ascii").replace("H|CARDTXN|01|", "H|CARDTXN|02|", 1), encoding="ascii")
    with pytest.raises(files.FileRejectedError, match="format version"):
        files.card_control(path)


def test_last_line_ignores_trailing_newline(tmp_path: Path) -> None:
    path = tmp_path / "f.dat"
    path.write_text("H|a\nD|b\nT|1|2\n", encoding="ascii")
    assert files.last_line(path) == "T|1|2"
    path.write_text("T|1|2", encoding="ascii")
    assert files.last_line(path) == "T|1|2"


def test_quarantine_keeps_a_copy_and_the_reason(tmp_path: Path) -> None:
    landed = files.landed(_card_file(tmp_path / "landing", cards.Defect.TRUNCATED))
    rejection = files.Rejection(landed.name, landed.sha256, "cards", "card-processor-transactions", "r1", "cut", "t")

    copy = files.quarantine(landed, rejection, tmp_path / "quarantine")
    again = files.quarantine(landed, rejection, tmp_path / "quarantine")

    assert copy == again
    assert copy.parent == tmp_path / "quarantine" / "cards"
    assert sha256_of(copy) == landed.sha256
    assert landed.path.exists()
    assert json.loads(copy.with_name(f"{copy.name}.rejection.json").read_text())["reason"] == "cut"
