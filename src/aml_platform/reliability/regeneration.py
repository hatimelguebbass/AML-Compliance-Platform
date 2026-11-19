"""Prove that the published derived layers are a function of bronze alone.

Silver, gold and the reference seeds are rebuilt from scratch on the staging branch, then
every table is compared with its published copy on ``main`` by row count and an
order-insensitive checksum of all its columns but the lineage ones. A difference means
the daily incremental loads and a full rebuild disagree: state carried between runs,
a non-deterministic expression, or a model changed without reprocessing the dates
already published. Bronze is the boundary: the change data capture topics are
compacted, so bronze holds the only full history of the core banking changes.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

PUBLISHED_CATALOG = "lakehouse"
STAGING_CATALOG = "lakehouse_etl"
# Observability holds run metadata and checks of each run, which a rebuild cannot reproduce.
DERIVED_NAMESPACES: tuple[str, ...] = ("reference", "silver", "restricted", "gold")
LINEAGE_COLUMNS = frozenset({"_pipeline_run_id", "_loaded_at"})


class Cursor(Protocol):
    def execute(self, operation: str, params: Sequence[Any] = ...) -> Any: ...

    def fetchall(self) -> list[Any]: ...


@dataclass(frozen=True)
class Fingerprint:
    rows: int
    checksum: str | None


@dataclass(frozen=True)
class TableComparison:
    namespace: str
    table: str
    published: Fingerprint | None
    rebuilt: Fingerprint | None
    columns_differ: bool = False

    @property
    def matches(self) -> bool:
        return not self.columns_differ and self.published is not None and self.published == self.rebuilt

    @property
    def verdict(self) -> str:
        if self.columns_differ:
            return "columns differ"
        if self.published is None:
            return "only rebuilt"
        if self.rebuilt is None:
            return "not rebuilt"
        if self.published.rows != self.rebuilt.rows:
            return "row counts differ"
        return "identical" if self.matches else "contents differ"


# Statements below are built from identifiers only, read from the information schema and
# quoted here; Trino cannot bind an identifier as a parameter.
def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def fingerprint_sql(catalog: str, namespace: str, table: str, columns: Iterable[str]) -> str:
    compared = [c for c in columns if c not in LINEAGE_COLUMNS]
    if not compared:
        raise ValueError(f"{namespace}.{table} has no column to compare")
    row = ", ".join(_quote(c) for c in compared)
    relation = f"{_quote(catalog)}.{_quote(namespace)}.{_quote(table)}"
    return f"SELECT count(*), to_hex(checksum(ROW({row}))) FROM {relation}"  # noqa: S608


def tables(cursor: Cursor, catalog: str, namespaces: Sequence[str]) -> dict[tuple[str, str], tuple[str, ...]]:
    """Columns of every table of ``namespaces`` in ``catalog``, in ordinal order."""
    placeholders = ", ".join("?" for _ in namespaces)
    query = (
        f"SELECT table_schema, table_name, column_name FROM {_quote(catalog)}.information_schema.columns"  # noqa: S608
        f" WHERE table_schema IN ({placeholders}) ORDER BY table_schema, table_name, ordinal_position"
    )
    cursor.execute(query, list(namespaces))
    found: dict[tuple[str, str], list[str]] = {}
    for namespace, table, column in cursor.fetchall():
        found.setdefault((namespace, table), []).append(column)
    return {key: tuple(columns) for key, columns in found.items()}


def fingerprint(cursor: Cursor, catalog: str, namespace: str, table: str, columns: Iterable[str]) -> Fingerprint:
    cursor.execute(fingerprint_sql(catalog, namespace, table, columns))
    [(rows, checksum)] = cursor.fetchall()
    return Fingerprint(int(rows), checksum)


def _count(cursor: Cursor, catalog: str, namespace: str, table: str) -> int:
    cursor.execute(f"SELECT count(*) FROM {_quote(catalog)}.{_quote(namespace)}.{_quote(table)}")  # noqa: S608
    [(rows,)] = cursor.fetchall()
    return int(rows)


def compare(
    cursor: Cursor,
    *,
    namespaces: Sequence[str] = DERIVED_NAMESPACES,
    published_catalog: str = PUBLISHED_CATALOG,
    rebuilt_catalog: str = STAGING_CATALOG,
) -> list[TableComparison]:
    published = tables(cursor, published_catalog, namespaces)
    rebuilt = tables(cursor, rebuilt_catalog, namespaces)
    results = []
    for namespace, table in sorted(published.keys() | rebuilt.keys()):
        published_columns = published.get((namespace, table))
        rebuilt_columns = rebuilt.get((namespace, table))
        if published_columns is None or rebuilt_columns is None:
            results.append(
                TableComparison(
                    namespace,
                    table,
                    None
                    if published_columns is None
                    else Fingerprint(_count(cursor, published_catalog, namespace, table), None),
                    None
                    if rebuilt_columns is None
                    else Fingerprint(_count(cursor, rebuilt_catalog, namespace, table), None),
                )
            )
            continue
        if set(published_columns) != set(rebuilt_columns):
            results.append(TableComparison(namespace, table, None, None, columns_differ=True))
            continue
        # Same column order on both sides, so the row checksums are comparable.
        results.append(
            TableComparison(
                namespace,
                table,
                fingerprint(cursor, published_catalog, namespace, table, published_columns),
                fingerprint(cursor, rebuilt_catalog, namespace, table, published_columns),
            )
        )
    return results


def drop_statements(cursor: Cursor, namespaces: Sequence[str] = DERIVED_NAMESPACES) -> list[str]:
    """DROP statements for every derived table of the staging branch, never of ``main``."""
    return [
        f"DROP TABLE {_quote(STAGING_CATALOG)}.{_quote(namespace)}.{_quote(table)}"
        for namespace, table in sorted(tables(cursor, STAGING_CATALOG, namespaces))
    ]
