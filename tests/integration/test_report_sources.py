"""Every column the semantic model imports is readable by bi on main, as Power BI reads it.

Needs gold and observability published (CI publishes the empty tables it built; locally,
`make pipeline`), hence its own marker.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aml_platform.reporting import pbip
from tests.integration.reporting import PASSWORDS, https_query, server_certificate

pytestmark = pytest.mark.reporting

PROJECT = Path(__file__).parents[2] / "bi"
SOURCES = pbip.source_columns(PROJECT)


@pytest.fixture(scope="module")
def certificate(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return server_certificate(tmp_path_factory.mktemp("trino"))


@pytest.mark.parametrize(("schema", "table"), sorted(SOURCES))
def test_bi_reads_every_column_the_model_imports(schema: str, table: str, certificate: Path) -> None:
    columns = ", ".join(SOURCES[(schema, table)])
    https_query("bi", PASSWORDS["bi"], f"SELECT {columns} FROM lakehouse.{schema}.{table} LIMIT 0", certificate)
