from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

import pytest

from aml_platform.reliability import regeneration
from aml_platform.reliability.regeneration import Fingerprint, TableComparison


class FakeTrino:
    """Answers the information schema and fingerprint queries from in-memory catalogs."""

    def __init__(self, catalogs: dict[str, dict[tuple[str, str], tuple[tuple[str, ...], int, str]]]) -> None:
        self.catalogs = catalogs
        self.statements: list[str] = []
        self._result: list[Any] = []

    def execute(self, operation: str, params: Sequence[Any] = ()) -> None:
        self.statements.append(operation)
        if columns := re.search(r'FROM "(\w+)"\.information_schema\.columns', operation):
            catalog = self.catalogs[columns.group(1)]
            self._result = [
                (namespace, table, column)
                for (namespace, table), (names, _, _) in sorted(catalog.items())
                if namespace in params
                for column in names
            ]
        elif relation := re.search(r'FROM "(\w+)"\."(\w+)"\."(\w+)"', operation):
            catalog, namespace, table = relation.groups()
            _names, rows, checksum = self.catalogs[catalog][(namespace, table)]
            self._result = [(rows, checksum)] if "checksum" in operation else [(rows,)]
        else:
            raise AssertionError(operation)

    def fetchall(self) -> list[Any]:
        return self._result


PARTY = ("party_id", "customer_count", "_pipeline_run_id", "_loaded_at")


def test_fingerprint_leaves_out_lineage_columns_and_quotes_identifiers() -> None:
    sql = regeneration.fingerprint_sql("lakehouse", "silver", "party", PARTY)
    assert sql == (
        'SELECT count(*), to_hex(checksum(ROW("party_id", "customer_count"))) FROM "lakehouse"."silver"."party"'
    )


def test_a_table_of_lineage_columns_only_cannot_be_compared() -> None:
    with pytest.raises(ValueError, match="no column to compare"):
        regeneration.fingerprint_sql("lakehouse", "silver", "t", ("_loaded_at",))


def test_identical_rebuild_matches() -> None:
    table = {("silver", "party"): (PARTY, 10, "ab")}
    [result] = regeneration.compare(FakeTrino({"lakehouse": table, "lakehouse_etl": table}))
    assert result.matches
    assert result.verdict == "identical"


@pytest.mark.parametrize(
    ("rebuilt", "verdict"),
    [
        ((PARTY, 10, "cd"), "contents differ"),
        ((PARTY, 11, "ab"), "row counts differ"),
        ((("party_id", "_loaded_at"), 10, "ab"), "columns differ"),
    ],
)
def test_a_rebuild_that_differs_is_reported(rebuilt: tuple[tuple[str, ...], int, str], verdict: str) -> None:
    trino = FakeTrino(
        {"lakehouse": {("silver", "party"): (PARTY, 10, "ab")}, "lakehouse_etl": {("silver", "party"): rebuilt}}
    )
    [result] = regeneration.compare(trino)
    assert not result.matches
    assert result.verdict == verdict


def test_a_table_on_one_side_only_is_reported() -> None:
    trino = FakeTrino(
        {
            "lakehouse": {("gold", "aml_alert"): (PARTY, 3, "ab")},
            "lakehouse_etl": {("gold", "aml_rule"): (PARTY, 7, "cd")},
        }
    )
    results = {r.table: r for r in regeneration.compare(trino)}
    assert results["aml_alert"].verdict == "not rebuilt"
    assert results["aml_alert"].published == Fingerprint(3, None)
    assert results["aml_rule"].verdict == "only rebuilt"


def test_only_staging_tables_are_ever_dropped() -> None:
    trino = FakeTrino({"lakehouse_etl": {("silver", "party"): (PARTY, 1, "x"), ("gold", "aml_alert"): (PARTY, 1, "y")}})
    statements = regeneration.drop_statements(trino)
    assert statements == [
        'DROP TABLE "lakehouse_etl"."gold"."aml_alert"',
        'DROP TABLE "lakehouse_etl"."silver"."party"',
    ]


def test_comparison_without_any_published_side_does_not_match() -> None:
    assert not TableComparison("silver", "party", None, Fingerprint(1, "x")).matches
