"""Creation and additive evolution of the Iceberg tables written by ingestion."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from aml_platform.ingestion.schema import quote

# Ingestion only ever writes to the staging branch; publishing to main is a merge after the audits.
CATALOG = "lakehouse_etl"

log = logging.getLogger(__name__)


class SchemaDriftError(RuntimeError):
    """The table and its definition disagree in a way that needs a migration, not an ALTER."""


@dataclass(frozen=True)
class TableSpec:
    namespace: str
    name: str
    columns: str
    partitioned_by: str | None = None
    comment: str = ""

    @property
    def name_in_catalog(self) -> str:
        """Catalog-independent name, as recorded in the audit tables: it stays valid after the merge to main."""
        return f"{self.namespace}.{self.name}"

    @property
    def identifier(self) -> str:
        return f"{CATALOG}.{self.name_in_catalog}"


def ensure_table(spark: SparkSession, spec: TableSpec) -> None:
    """Create the table, or add the columns its definition gained since it was created.

    Columns are never dropped or retyped automatically: that would rewrite history
    that downstream models may depend on.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{spec.namespace}")
    if not spark.catalog.tableExists(spec.identifier):
        partitioning = f" PARTITIONED BY ({spec.partitioned_by})" if spec.partitioned_by else ""
        comment = spec.comment.replace("'", "''")
        spark.sql(
            f"CREATE TABLE {spec.identifier} ({spec.columns}) USING iceberg{partitioning}"
            f" COMMENT '{comment}'"
            " TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')"
        )
        log.info("created %s", spec.identifier)
        return

    wanted = StructType.fromDDL(spec.columns)
    if not isinstance(wanted, StructType):
        raise TypeError(f"{spec.identifier}: column definition is not a struct")
    existing = {f.name: f.dataType for f in spark.table(spec.identifier).schema.fields}
    added = []
    for field in wanted.fields:
        if field.name not in existing:
            added.append(f"{quote(field.name)} {field.dataType.simpleString()}")
        elif existing[field.name] != field.dataType:
            raise SchemaDriftError(
                f"{spec.identifier}.{field.name} is {existing[field.name].simpleString()}, "
                f"definition says {field.dataType.simpleString()}"
            )
    if added:
        spark.sql(f"ALTER TABLE {spec.identifier} ADD COLUMNS ({', '.join(added)})")
        log.info("added columns to %s: %s", spec.identifier, ", ".join(added))
