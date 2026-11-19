from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import pytest

from aml_platform.sources import cards, cli, ibm_aml
from aml_platform.sources.landing import sha256_of


def test_defect_option_is_parsed() -> None:
    args = cli.build_parser().parse_args(
        ["cards", "--defect", "2022-09-05:truncated", "--defect", "2022-09-07:duplicates"]
    )
    assert args.defect == [(date(2022, 9, 5), cards.Defect.TRUNCATED), (date(2022, 9, 7), cards.Defect.DUPLICATES)]


def test_unknown_defect_is_a_usage_error() -> None:
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["cards", "--defect", "2022-09-05:lost"])


def test_operator_errors_exit_cleanly(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["--data-dir", str(tmp_path), "ibm", "--from-dir", str(tmp_path / "missing")])
    assert exit_info.value.code == 1
    assert "missing" in caplog.text


def test_payments_runs_end_to_end_on_verified_files(
    ibm_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixtures = tuple(ibm_aml.SourceFile(f.name, sha256_of(ibm_dir / f.name)) for f in ibm_aml.FILES)
    monkeypatch.setattr(ibm_aml, "FILES", fixtures)
    monkeypatch.setattr(ibm_aml, "TRANSACTIONS", fixtures[0])
    monkeypatch.setattr(ibm_aml, "PATTERNS", fixtures[2])

    shutil.copytree(ibm_dir, tmp_path / "raw" / "ibm")
    cli.main(["--data-dir", str(tmp_path), "payments"])

    assert sorted(p.name for p in (tmp_path / "landing" / "payments").glob("*.csv")) == [
        "PAYMENTS_20220901.csv",
        "PAYMENTS_20220902.csv",
    ]
    assert (tmp_path / "ground_truth" / "laundering_transfers.csv").is_file()
