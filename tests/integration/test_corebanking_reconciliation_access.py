"""The lakehouse reads the core banking only to reconcile it: through one catalog, open to
dbt alone, with a database role that cannot see names, documents or contact details."""

from __future__ import annotations

import os

import pytest
import trino

pytestmark = pytest.mark.integration

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")


def _query(user: str, sql: str) -> list[tuple[object, ...]]:
    connection = trino.dbapi.connect(host=TRINO_HOST, port=8080, user=user)
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        connection.close()


def test_dbt_reads_the_columns_silver_keeps() -> None:
    rows = _query("dbt", "SELECT customer_id, risk_rating, is_pep, updated_at FROM corebanking.core.customer LIMIT 1")
    assert len(rows) <= 1


@pytest.mark.parametrize("column", ["first_name", "last_name", "id_document_number", "tax_id", "phone", "email"])
def test_identities_stay_out_of_reach_of_the_reconciliation_role(column: str) -> None:
    with pytest.raises(trino.exceptions.TrinoQueryError, match="permission denied"):
        _query("dbt", f"SELECT {column} FROM corebanking.core.customer LIMIT 1")


@pytest.mark.parametrize("user", ["analyst", "bi", "compliance", "integration-test"])
def test_no_other_user_sees_the_core_banking_catalog(user: str) -> None:
    with pytest.raises(trino.exceptions.TrinoUserError, match="Access Denied"):
        _query(user, "SELECT customer_id FROM corebanking.core.customer LIMIT 1")


def test_the_catalog_is_read_only() -> None:
    with pytest.raises(trino.exceptions.TrinoUserError, match="Access Denied"):
        _query("dbt", "DELETE FROM corebanking.core.card WHERE card_id = -1")
