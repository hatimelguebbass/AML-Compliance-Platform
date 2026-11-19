"""End-to-end run of the Spark ingestion jobs on a small landing zone.

Runs the real container image against the running stack, in throw-away
namespaces on the staging branch, which is reset afterwards.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pytest
import trino

from aml_platform.lakehouse.nessie import NessieClient
from aml_platform.sources import cards, payments
from aml_platform.sources.landing import Manifest, sha256_of

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures"
NESSIE_URL = os.environ.get("NESSIE_URL", "http://localhost:19120")
TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
HOLDERS = [cards.CardHolder(f"9{i:015d}", city) for i, city in enumerate(["Rabat", "Fes", "Agadir"] * 10)]
CLEAN_DAY, RESENT_DAY, TRUNCATED_DAY = date(2022, 9, 1), date(2022, 9, 2), date(2022, 9, 3)
RELEASE_DAY = date(2022, 9, 4)
CARDS_PER_FILE = 300
SANCTIONS_ENTRIES = {"un": 3, "ofac": 3}


@dataclass(frozen=True)
class Landing:
    root: Path
    quarantine: Path
    bronze: str
    audit: str

    def run(self, command: str, *arguments: str) -> subprocess.CompletedProcess[str]:
        env = {
            **os.environ,
            "LANDING_DIR": str(self.root),
            "QUARANTINE_DIR": str(self.quarantine),
            "HOST_UID": str(os.getuid()),
            "HOST_GID": str(os.getgid()),
        }
        submit = ["/opt/spark/bin/spark-submit", "jobs/ingest.py", command, *arguments]
        namespaces = ["--bronze-namespace", self.bronze, "--audit-namespace", self.audit]
        return subprocess.run(
            ["docker", "compose", "--profile", "spark", "run", "--rm", "spark", *submit, *namespaces],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )


def _land_card_file(directory: Path, day: date, defects: frozenset[cards.Defect] = frozenset()) -> None:
    merchants = cards.build_merchants([h.home_city for h in HOLDERS], seed=7, per_city=20)
    cards.generate_file(directory, day, HOLDERS, merchants, count=CARDS_PER_FILE, seed=7, defects=defects)


def _land_cards(directory: Path) -> None:
    for day, defects in (
        (CLEAN_DAY, frozenset()),
        (RESENT_DAY, frozenset({cards.Defect.DUPLICATES})),
        (TRUNCATED_DAY, frozenset({cards.Defect.TRUNCATED})),
    ):
        _land_card_file(directory, day, defects)


def _land_sanctions(directory: Path) -> None:
    for name, entries in SANCTIONS_ENTRIES.items():
        target = directory / name / f"{name}_20261101T060000Z.xml"
        target.parent.mkdir(parents=True)
        shutil.copyfile(FIXTURES / "sanctions" / f"{name}.xml", target)
        attributes = {"list": name, "publication": "2026-11-01", "retrieved_at": "20261101T060000Z", "source_url": ""}
        Manifest(target.name, entries, sha256_of(target), attributes).write_next_to(target)


@pytest.fixture(scope="module")
def landing(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Landing]:
    root = tmp_path_factory.mktemp("landing")
    payments.build_feed(
        FIXTURES / "ibm" / "HI-Small_Trans.csv",
        FIXTURES / "ibm" / "HI-Small_Patterns.txt",
        root / "payments",
        tmp_path_factory.mktemp("ground_truth"),
    )
    # Delivered without its manifest yet: must wait, not be read.
    (root / "payments" / "PAYMENTS_20220903.csv").write_text(",".join(payments.FEED_COLUMNS) + "\n")
    _land_cards(root / "cards")
    _land_sanctions(root / "sanctions")
    quarantine = tmp_path_factory.mktemp("quarantine")
    suffix = uuid.uuid4().hex[:8]

    with NessieClient(NESSIE_URL) as nessie:
        nessie.ensure_branch_at("etl")
        try:
            yield Landing(root, quarantine, f"it_bronze_{suffix}", f"it_audit_{suffix}")
        finally:
            nessie.ensure_branch_at("etl")


@pytest.fixture(scope="module")
def first_run(landing: Landing) -> subprocess.CompletedProcess[str]:
    return landing.run("files")


@pytest.fixture(scope="module")
def cursor() -> Iterator[trino.dbapi.Cursor]:
    connection = trino.dbapi.connect(host=TRINO_HOST, port=8080, user="integration-test", catalog="lakehouse_etl")
    try:
        yield connection.cursor()
    finally:
        connection.close()


def _scalar(cursor: trino.dbapi.Cursor, sql: str) -> object:
    cursor.execute(sql)
    return cursor.fetchall()[0][0]


def _rows(cursor: trino.dbapi.Cursor, sql: str) -> list[list[object]]:
    cursor.execute(sql)
    return [list(row) for row in cursor.fetchall()]


def test_ingestion_run_succeeds(first_run: subprocess.CompletedProcess[str]) -> None:
    assert first_run.returncode == 0, first_run.stderr[-4000:]


def test_payments_are_typed_and_partitioned_by_business_date(
    first_run: subprocess.CompletedProcess[str], landing: Landing, cursor: trino.dbapi.Cursor
) -> None:
    rows = _rows(
        cursor,
        f"SELECT _business_date, count(*), min(typeof(amount_paid)), min(typeof(booked_at))"
        f" FROM {landing.bronze}.payment_transfer GROUP BY 1 ORDER BY 1",
    )
    assert rows == [
        [date(2022, 9, 1), 6, "decimal(22,6)", "timestamp(6)"],
        [date(2022, 9, 2), 3, "decimal(22,6)", "timestamp(6)"],
    ]


def test_card_resends_are_dropped_and_truncated_file_is_quarantined(
    first_run: subprocess.CompletedProcess[str], landing: Landing, cursor: trino.dbapi.Cursor
) -> None:
    per_day = _rows(
        cursor,
        f"SELECT _business_date, count(*), count(DISTINCT txn_ref)"
        f" FROM {landing.bronze}.card_transaction GROUP BY 1 ORDER BY 1",
    )
    assert per_day == [[CLEAN_DAY, CARDS_PER_FILE, CARDS_PER_FILE], [RESENT_DAY, CARDS_PER_FILE, CARDS_PER_FILE]]

    outcomes = _rows(
        cursor,
        f"SELECT file_name, status, duplicates_dropped FROM {landing.audit}.ingestion_file"
        f" WHERE source = 'cards' ORDER BY file_name",
    )
    assert outcomes == [
        [cards.file_name(CLEAN_DAY), "ACCEPTED", 0],
        [cards.file_name(RESENT_DAY), "ACCEPTED", CARDS_PER_FILE // 200],
        [cards.file_name(TRUNCATED_DAY), "REJECTED", 0],
    ]
    quarantined = sorted(p.name for p in (landing.quarantine / "cards").iterdir())
    assert len(quarantined) == 2
    assert quarantined[0].endswith(cards.file_name(TRUNCATED_DAY))
    assert (landing.root / "cards" / cards.file_name(TRUNCATED_DAY)).exists()


def test_sanctions_snapshots_keep_nested_structure(
    first_run: subprocess.CompletedProcess[str], landing: Landing, cursor: trino.dbapi.Cursor
) -> None:
    assert _scalar(cursor, f"SELECT count(*) FROM {landing.bronze}.sanctions_un_individual") == 2
    assert _scalar(cursor, f"SELECT count(*) FROM {landing.bronze}.sanctions_un_entity") == 1
    aliases = _rows(
        cursor,
        f"SELECT uid, programList.program, transform(akaList.aka, a -> a.lastName)"
        f" FROM {landing.bronze}.sanctions_ofac_sdn_entry WHERE uid = 100",
    )
    assert aliases == [[100, ["SDGT"], ["EX SHIP"]]]


def test_file_without_manifest_waits(
    first_run: subprocess.CompletedProcess[str], landing: Landing, cursor: trino.dbapi.Cursor
) -> None:
    pending = _scalar(cursor, f"SELECT files_pending FROM {landing.audit}.ingestion_run WHERE source = 'payments'")
    assert pending == 1


def test_rerun_writes_nothing_and_audit_passes(
    first_run: subprocess.CompletedProcess[str], landing: Landing, cursor: trino.dbapi.Cursor
) -> None:
    def counts() -> list[list[object]]:
        return _rows(
            cursor,
            f"SELECT (SELECT count(*) FROM {landing.bronze}.payment_transfer),"
            f" (SELECT count(*) FROM {landing.bronze}.card_transaction),"
            f" (SELECT count(*) FROM {landing.bronze}.sanctions_ofac_sdn_entry)",
        )

    before = counts()
    rerun = landing.run("files")
    assert rerun.returncode == 0, rerun.stderr[-4000:]
    assert counts() == before

    skipped = _rows(
        cursor,
        f"SELECT source, files_skipped, records_written FROM {landing.audit}.ingestion_run"
        f" WHERE run_id <> (SELECT min_by(run_id, started_at) FROM {landing.audit}.ingestion_run) ORDER BY source",
    )
    assert skipped == [["cards", 3, 0], ["payments", 2, 0], ["sanctions_ofac", 1, 0], ["sanctions_un", 1, 0]]

    audit = landing.run("audit")
    assert audit.returncode == 0, audit.stderr[-4000:]
    failed = _scalar(cursor, f"SELECT count_if(NOT passed) FROM {landing.audit}.publish_check")
    assert failed == 0


def test_rejected_file_holds_its_business_date_until_redelivered(
    first_run: subprocess.CompletedProcess[str], landing: Landing, cursor: trino.dbapi.Cursor
) -> None:
    run = ["--business-date", RELEASE_DAY.isoformat()]
    audit = ["audit", "--pipeline-run", "it-release", "--pipeline-feeds", "cards", *run]
    _land_card_file(landing.root / "cards", RELEASE_DAY, frozenset({cards.Defect.DATE_FORMAT}))

    ingest = landing.run("cards", *run, "--run-id", "it-release#1")
    assert ingest.returncode == 0, ingest.stderr[-4000:]
    reason = _scalar(cursor, f"SELECT reason FROM {landing.audit}.ingestion_file WHERE run_id = 'it-release#1'")
    assert reason == f"txn_local_time:not_timestamp={CARDS_PER_FILE}"
    assert landing.run(*audit).returncode == 1
    detail = _scalar(
        cursor,
        f"SELECT detail FROM {landing.audit}.publish_check"
        f" WHERE check_name = 'business_date_delivered:cards' AND NOT passed",
    )
    assert detail == f"{cards.file_name(RELEASE_DAY)} rejected: {reason}"

    # The processor resends the date in the agreed format and the run is resumed.
    _land_card_file(landing.root / "cards", RELEASE_DAY)
    resumed = landing.run("cards", *run, "--run-id", "it-release#2")
    assert resumed.returncode == 0, resumed.stderr[-4000:]
    passed = landing.run(*audit)
    assert passed.returncode == 0, passed.stderr[-4000:]
    assert _rows(
        cursor,
        f"SELECT count(*) FROM {landing.bronze}.card_transaction WHERE _business_date = DATE '{RELEASE_DAY}'",
    ) == [[CARDS_PER_FILE]]
