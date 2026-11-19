"""Change data capture end to end: PostgreSQL -> Debezium -> Kafka -> Spark -> bronze.

Captures a throw-away schema with its own connector, slot, publication and
topics, ingests into throw-away namespaces on the staging branch, and removes
everything afterwards.
"""

from __future__ import annotations

import os
import subprocess
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import psycopg
import pytest
import trino
from psycopg import sql

from aml_platform.cdc import connect, kafka_admin, source_db
from aml_platform.cdc.topics import CdcSource
from aml_platform.lakehouse.nessie import NessieClient
from aml_platform.sources import corebanking

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
NESSIE_URL = os.environ.get("NESSIE_URL", "http://localhost:19120")
TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
CONNINFO = psycopg.conninfo.make_conninfo(
    host=os.environ.get("SOURCE_DB_HOST", "localhost"),
    port=os.environ.get("SOURCE_DB_PORT", "5432"),
    dbname=os.environ.get("SOURCE_DB_NAME", "corebanking"),
    user=os.environ.get("SOURCE_DB_USER", "corebanking"),
    password=os.environ.get("SOURCE_DB_PASSWORD", "corebanking-local"),
)
CDC_USER = os.environ.get("CDC_DB_USER", "debezium")
CDC_PASSWORD = os.environ.get("CDC_DB_PASSWORD", "debezium-local")


@dataclass(frozen=True)
class Capture:
    source: CdcSource
    data: corebanking.CoreBankingData
    bronze: str
    audit: str

    def run(self, command: str) -> subprocess.CompletedProcess[str]:
        env = {**os.environ, "HOST_UID": str(os.getuid()), "HOST_GID": str(os.getgid())}
        arguments = [
            "--bronze-namespace",
            self.bronze,
            "--audit-namespace",
            self.audit,
            "--cdc-topic-prefix",
            self.source.topic_prefix,
            "--cdc-schema",
            self.source.schema,
        ]
        return subprocess.run(
            [
                *("docker", "compose", "--profile", "spark", "run", "--rm", "spark"),
                *("/opt/spark/bin/spark-submit", "jobs/ingest.py", command),
                *arguments,
            ],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )

    def execute(self, statement: str) -> None:
        with psycopg.connect(CONNINFO, autocommit=True) as connection:
            connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(self.source.schema)))
            connection.execute(statement.encode())

    def wait_for_events(self, table: str, count: int, timeout: float = 120.0) -> None:
        topic = self.source.change_topic(table)
        deadline = time.monotonic() + timeout
        while (total := sum(kafka_admin.end_offsets(topic).values())) < count:
            if time.monotonic() > deadline:
                pytest.fail(f"{topic}: {total} events after {timeout:.0f}s, expected {count}")
            time.sleep(1)


@pytest.fixture(scope="module")
def capture() -> Iterator[Capture]:
    ibm_dir = ROOT / "tests" / "fixtures" / "ibm"
    suffix = uuid.uuid4().hex[:8]
    schema = f"cdc_test_{suffix}"
    source = CdcSource(topic_prefix=f"it_{suffix}", schema=schema, tables=("customer", "account", "card"))
    data = corebanking.generate(
        corebanking.read_ibm_accounts(ibm_dir / "HI-Small_accounts.csv"),
        corebanking.account_currencies(ibm_dir / "HI-Small_Trans.csv"),
        [],
        corebanking.GenerationConfig(duplicate_rate_person=0.0, card_rate=1.0, sanctioned_customers=0),
    )
    corebanking.load(CONNINFO, data, schema=schema)
    try:
        source_db.prepare(CONNINFO, source, role=CDC_USER, password=CDC_PASSWORD)
        kafka_admin.ensure_topics(source.topics())
        with connect.KafkaConnect(CONNECT_URL) as client, NessieClient(NESSIE_URL) as nessie:
            nessie.ensure_branch_at("etl")
            try:
                client.apply(source.connector_name, connect.connector_config(source))
                client.wait_until_running(source.connector_name)
                yield Capture(source, data, f"it_bronze_{suffix}", f"it_audit_{suffix}")
            finally:
                client.remove(source.connector_name)
                nessie.ensure_branch_at("etl")
    finally:
        source_db.drop(CONNINFO, source)
        kafka_admin.delete_topics([t.name for t in source.topics()])
        with psycopg.connect(CONNINFO, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture(scope="module")
def cursor() -> Iterator[trino.dbapi.Cursor]:
    connection = trino.dbapi.connect(host=TRINO_HOST, port=8080, user="integration-test", catalog="lakehouse_etl")
    try:
        yield connection.cursor()
    finally:
        connection.close()


def _rows(cursor: trino.dbapi.Cursor, query: str) -> list[list[object]]:
    cursor.execute(query)
    return [list(row) for row in cursor.fetchall()]


@pytest.fixture(scope="module")
def snapshot_run(capture: Capture) -> subprocess.CompletedProcess[str]:
    capture.wait_for_events("customer", len(capture.data.customers))
    capture.wait_for_events("card", len(capture.data.cards))
    return capture.run("corebanking")


@pytest.fixture(scope="module")
def changes_run(capture: Capture, snapshot_run: subprocess.CompletedProcess[str]) -> subprocess.CompletedProcess[str]:
    assert snapshot_run.returncode == 0, snapshot_run.stderr[-4000:]
    customer = capture.data.customers[0].customer_id
    card = capture.data.cards[0].card_id
    capture.execute(f"UPDATE customer SET risk_rating = 'HIGH' WHERE customer_id = {customer}")
    capture.execute(f"UPDATE customer SET is_pep = true WHERE customer_id = {customer}")
    capture.execute(f"DELETE FROM card WHERE card_id = {card}")
    capture.wait_for_events("customer", len(capture.data.customers) + 2)
    # A delete is followed by its tombstone.
    capture.wait_for_events("card", len(capture.data.cards) + 2)
    return capture.run("corebanking")


def test_snapshot_lands_every_row_typed(
    capture: Capture, snapshot_run: subprocess.CompletedProcess[str], cursor: trino.dbapi.Cursor
) -> None:
    assert snapshot_run.returncode == 0, snapshot_run.stderr[-4000:]
    rows = _rows(
        cursor,
        f"SELECT _op, count(*), count(DISTINCT customer_id), min(typeof(birth_date)), min(typeof(updated_at))"
        f" FROM {capture.bronze}.corebanking_customer GROUP BY 1",
    )
    customers = len(capture.data.customers)
    assert rows == [["r", customers, customers, "date", "timestamp(6) with time zone"]]
    expected = capture.data.customers[0]
    sample = _rows(
        cursor,
        f"SELECT customer_since, created_at FROM {capture.bronze}.corebanking_customer"
        f" WHERE customer_id = {expected.customer_id}",
    )
    assert sample == [[expected.customer_since, expected.created_at]]


def test_updates_and_deletes_are_appended_in_order(
    capture: Capture, changes_run: subprocess.CompletedProcess[str], cursor: trino.dbapi.Cursor
) -> None:
    assert changes_run.returncode == 0, changes_run.stderr[-4000:]
    customer = capture.data.customers[0].customer_id
    history = _rows(
        cursor,
        f"SELECT _op, risk_rating, is_pep FROM {capture.bronze}.corebanking_customer"
        f" WHERE customer_id = {customer} ORDER BY _source_lsn, _kafka_offset",
    )
    original = capture.data.customers[0]
    assert history == [
        ["r", original.risk_rating, original.is_pep],
        ["u", "HIGH", original.is_pep],
        ["u", "HIGH", True],
    ]
    card = capture.data.cards[0].card_id
    deleted = _rows(
        cursor,
        f"SELECT _op, card_token, status FROM {capture.bronze}.corebanking_card"
        f" WHERE card_id = {card} ORDER BY _kafka_offset",
    )
    original_card = capture.data.cards[0]
    assert deleted == [
        ["r", original_card.card_token, original_card.status],
        ["d", original_card.card_token, original_card.status],
    ]


def test_rerun_writes_nothing_and_audit_passes(
    capture: Capture, changes_run: subprocess.CompletedProcess[str], cursor: trino.dbapi.Cursor
) -> None:
    def counts() -> list[list[object]]:
        return _rows(
            cursor,
            f"SELECT (SELECT count(*) FROM {capture.bronze}.corebanking_customer),"
            f" (SELECT count(*) FROM {capture.bronze}.corebanking_account),"
            f" (SELECT count(*) FROM {capture.bronze}.corebanking_card)",
        )

    before = counts()
    rerun = capture.run("corebanking")
    assert rerun.returncode == 0, rerun.stderr[-4000:]
    assert counts() == before

    audit = capture.run("audit")
    assert audit.returncode == 0, audit.stderr[-4000:]
    checks = _rows(
        cursor,
        f"SELECT check_name, target_table, passed FROM {capture.audit}.publish_check ORDER BY 2, 1",
    )
    assert checks == [
        [check, f"{capture.bronze}.corebanking_{table}", True]
        for table in ("account", "card", "customer")
        for check in ("matches_change_batches", "unique_key")
    ]


def test_source_schema_change_stops_ingestion(
    capture: Capture, changes_run: subprocess.CompletedProcess[str], cursor: trino.dbapi.Cursor
) -> None:
    before = _rows(cursor, f"SELECT count(*) FROM {capture.bronze}.corebanking_customer")
    customer = capture.data.customers[1].customer_id
    capture.execute("ALTER TABLE customer ADD COLUMN nickname text")
    capture.execute(f"UPDATE customer SET nickname = 'Momo' WHERE customer_id = {customer}")
    capture.wait_for_events("customer", len(capture.data.customers) + 3)

    run = capture.run("corebanking")
    assert run.returncode != 0
    output = run.stdout + run.stderr
    assert "fields_differ_from_contract=1" in output, output[-4000:]
    assert _rows(cursor, f"SELECT count(*) FROM {capture.bronze}.corebanking_customer") == before
    failed = _rows(
        cursor,
        f"SELECT status FROM {capture.audit}.ingestion_run WHERE source = 'corebanking'"
        f" ORDER BY finished_at DESC LIMIT 1",
    )
    assert failed == [["FAILED"]]
