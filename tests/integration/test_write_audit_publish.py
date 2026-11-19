"""Proves the isolation guarantee the pipelines rely on: data written on the
staging branch is invisible to readers of ``main`` until it is merged."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pytest
import trino

from aml_platform.lakehouse.nessie import NessieClient

pytestmark = pytest.mark.integration

NESSIE_URL = os.environ.get("NESSIE_URL", "http://localhost:19120")
TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")


@pytest.fixture
def cursor() -> Iterator[trino.dbapi.Cursor]:
    connection = trino.dbapi.connect(host=TRINO_HOST, port=8080, user="integration-test")
    try:
        yield connection.cursor()
    finally:
        connection.close()


@pytest.fixture
def nessie() -> Iterator[NessieClient]:
    with NessieClient(NESSIE_URL) as client:
        yield client


def _tables(cursor: trino.dbapi.Cursor, catalog: str) -> set[str]:
    cursor.execute(f"SHOW TABLES FROM {catalog}.audit")
    return {row[0] for row in cursor.fetchall()}


def test_staged_table_is_published_only_after_merge(cursor: trino.dbapi.Cursor, nessie: NessieClient) -> None:
    table = f"wap_check_{uuid.uuid4().hex[:8]}"
    nessie.ensure_branch_at("etl")

    cursor.execute(f"CREATE TABLE lakehouse_etl.audit.{table} AS SELECT 42 AS answer")
    cursor.fetchall()

    assert table in _tables(cursor, "lakehouse_etl")
    assert table not in _tables(cursor, "lakehouse")

    nessie.merge("etl", message=f"integration test {table}")
    try:
        cursor.execute(f"SELECT answer FROM lakehouse.audit.{table}")
        assert cursor.fetchall() == [[42]]
    finally:
        cursor.execute(f"DROP TABLE lakehouse.audit.{table}")
        cursor.fetchall()
        nessie.ensure_branch_at("etl")
