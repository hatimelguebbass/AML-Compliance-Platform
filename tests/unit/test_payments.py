from __future__ import annotations

import csv
import shutil
from pathlib import Path

import pytest

from aml_platform.sources import payments
from aml_platform.sources.landing import Manifest, sha256_of


def _build(ibm_dir: Path, out: Path) -> payments.FeedSummary:
    return payments.build_feed(
        ibm_dir / "HI-Small_Trans.csv", ibm_dir / "HI-Small_Patterns.txt", out / "landing", out / "truth"
    )


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def test_one_file_per_business_date_with_manifest(ibm_dir: Path, tmp_path: Path) -> None:
    summary = _build(ibm_dir, tmp_path)

    assert [f.name for f in summary.files] == ["PAYMENTS_20220901.csv", "PAYMENTS_20220902.csv"]
    assert summary.record_count == 9
    for data_file in summary.files:
        manifest = Manifest.read(Manifest.path_for(data_file))
        assert manifest.sha256 == sha256_of(data_file)
        assert manifest.record_count == len(_rows(data_file))
    assert not list((tmp_path / "landing").glob(".*partial"))


def test_feed_is_converted_and_keeps_bank_ids_as_delivered(ibm_dir: Path, tmp_path: Path) -> None:
    _build(ibm_dir, tmp_path)
    rows = _rows(tmp_path / "landing" / "PAYMENTS_20220901.csv")

    assert tuple(rows[0]) == payments.FEED_COLUMNS
    assert rows[1]["originator_bank_id"] == "0010"
    assert rows[1]["booked_at"] == "2022-09-01T00:21:00"
    assert {r["payment_currency"] for r in rows} >= {"USD", "EUR", "XBT"}
    wire = rows[5]
    assert (wire["amount_paid"], wire["payment_currency"]) == ("17.40", "EUR")
    assert (wire["amount_received"], wire["receiving_currency"]) == ("15.00", "GBP")
    assert wire["payment_format"] == "WIRE"


def test_label_is_only_in_ground_truth(ibm_dir: Path, tmp_path: Path) -> None:
    summary = _build(ibm_dir, tmp_path)
    truth = _rows(tmp_path / "truth" / "laundering_transfers.csv")

    assert summary.laundering_count == len(truth) == 3
    assert {(t["transfer_id"], t["typology"]) for t in truth} == {
        ("TRF000000003", "CYCLE"),
        ("TRF000000004", "CYCLE"),
        ("TRF000000009", ""),
    }
    assert summary.unmatched_pattern_rows == 0
    for data_file in summary.files:
        assert tuple(_rows(data_file)[0]) == payments.FEED_COLUMNS


def test_identical_source_rows_get_distinct_transfer_ids(ibm_dir: Path, tmp_path: Path) -> None:
    _build(ibm_dir, tmp_path)
    ids = [r["transfer_id"] for r in _rows(tmp_path / "landing" / "PAYMENTS_20220902.csv")]
    assert ids == ["TRF000000007", "TRF000000008", "TRF000000009"]


def test_output_is_deterministic(ibm_dir: Path, tmp_path: Path) -> None:
    first = _build(ibm_dir, tmp_path / "a")
    second = _build(ibm_dir, tmp_path / "b")
    assert [sha256_of(f) for f in first.files] == [sha256_of(f) for f in second.files]


@pytest.mark.parametrize(
    ("bad_value", "column", "message"),
    [
        ("Peso Oro", 6, "unmapped code"),
        ("01/09/2022 00:20", 0, "bad timestamp"),
        ("12,5", 5, "expected 11 fields"),
        ("1e5", 7, "bad amount"),
    ],
)
def test_unexpected_values_fail_the_build(
    ibm_dir: Path, tmp_path: Path, bad_value: str, column: int, message: str
) -> None:
    source = tmp_path / "ibm"
    shutil.copytree(ibm_dir, source)
    lines = (source / "HI-Small_Trans.csv").read_text().splitlines()
    fields = lines[1].split(",")
    fields[column] = bad_value
    lines[1] = ",".join(fields)
    (source / "HI-Small_Trans.csv").write_text("\n".join(lines) + "\n")

    with pytest.raises(payments.SourceFormatError, match=message):
        _build(source, tmp_path / "out")
    assert not list((tmp_path / "out" / "landing").glob("*.csv"))
