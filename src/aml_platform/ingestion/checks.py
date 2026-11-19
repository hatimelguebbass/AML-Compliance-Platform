"""Audit step of write-audit-publish: table-level checks on the staging branch.

File-level and event-level contract checks already ran during ingestion. These
checks look at each bronze table as a whole, as consumers will see it after the
merge: keys are unique across files (and change events across offsets), and
every table holds exactly the records the audit tables say were written, file
by file or topic partition by partition. For an orchestrated run, the branch must
also hold the successful ingestion of every feed of that run: a branch reset by
another run in the meantime would otherwise publish a business date with feeds
missing. The run's business date must also hold an accepted file of each dated
feed: a rejected file leaves its ingestion successful and the date without that
feed, which would otherwise only show in the volume checks, after silver and gold
were built on it. A failure blocks the publish.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from aml_platform import contracts
from aml_platform.ingestion import cards, corebanking, payments, sanctions
from aml_platform.ingestion.audit import Status, ingestion_file_table, ingestion_run_table, utcnow
from aml_platform.ingestion.tables import CATALOG, TableSpec, ensure_table

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class BronzeTable:
    name: str
    key: tuple[str, ...]
    # True when a redelivered file replaces the rows of its previous version, False when rows accumulate.
    replaced_per_file: bool


@dataclass(frozen=True)
class PipelineRun:
    """An orchestrator run: its ingestions record run ids ``<run_id>#<attempt>``."""

    run_id: str
    feeds: tuple[str, ...]
    business_date: date | None = None


@dataclass(frozen=True)
class CheckResult:
    check: str
    table: str
    passed: bool
    detail: str


def bronze_tables() -> list[BronzeTable]:
    sanctions_contract = contracts.load_by_id(sanctions.CONTRACT_ID)
    return [
        BronzeTable(payments.TABLE, ("transfer_id",), replaced_per_file=True),
        BronzeTable(cards.TABLE, (cards.KEY,), replaced_per_file=False),
        *(
            BronzeTable(sanctions.table_name(obj), (*obj.primary_key, "_retrieved_at"), replaced_per_file=True)
            for obj in sanctions_contract.objects
        ),
    ]


def sources_of(feed: str) -> tuple[str, ...]:
    """The source names under which ``ingest <feed>`` records its runs."""
    if feed == "sanctions":
        return tuple(f.source for f in sanctions.FEEDS)
    return {"payments": (payments.SOURCE,), "cards": (cards.SOURCE,), "corebanking": (corebanking.SOURCE,)}[feed]


# Feeds that deliver one file per business date; sanctions snapshots and change events are not dated.
DATED_FEEDS = ("payments", "cards")


def change_log_tables() -> list[str]:
    contract = contracts.load_by_id(corebanking.CONTRACT_ID)
    return [corebanking.table_name(obj) for obj in contract.objects]


def publish_check_table(namespace: str) -> TableSpec:
    return TableSpec(
        namespace=namespace,
        name="publish_check",
        columns="run_id STRING, check_name STRING, target_table STRING, passed BOOLEAN, detail STRING,"
        " checked_at TIMESTAMP",
        comment="Table-level checks run on the staging branch before each publish",
    )


def _unique_key(table: DataFrame, spec: BronzeTable, name: str) -> CheckResult:
    repeated = table.groupBy(*spec.key).count().where(F.col("count") > 1).count()
    return CheckResult("unique_key", name, repeated == 0, f"{repeated} keys on {'+'.join(spec.key)} repeated")


def _matches_registry(table: DataFrame, registry: DataFrame, spec: BronzeTable, name: str) -> CheckResult:
    accepted = registry.where((F.col("target_table") == name) & (F.col("status") == Status.ACCEPTED.value))
    if spec.replaced_per_file:
        latest = Window.partitionBy("file_name").orderBy(F.col("processed_at").desc())
        accepted = accepted.withColumn("_rank", F.row_number().over(latest)).where(F.col("_rank") == 1)
    expected = accepted.groupBy(F.col("file_name").alias("_source_file")).agg(
        F.sum("records_written").alias("expected")
    )
    actual = table.groupBy("_source_file").agg(F.count(F.lit(1)).alias("actual"))
    mismatched = (
        expected.join(actual, "_source_file", "full_outer")
        .where(~F.coalesce(F.col("expected"), F.lit(0)).eqNullSafe(F.coalesce(F.col("actual"), F.lit(0))))
        .orderBy("_source_file")
        .collect()
    )
    detail = "; ".join(f"{r._source_file}: registry {r.expected}, table {r.actual}" for r in mismatched[:5])
    return CheckResult("matches_registry", name, not mismatched, detail or "every file reconciles")


def _matches_change_batches(table: DataFrame, batches: DataFrame, name: str) -> CheckResult:
    position = ["_kafka_topic", "_kafka_partition"]
    expected = (
        batches.where(F.col("target_table") == name)
        .select(
            F.col("topic").alias("_kafka_topic"), F.col("kafka_partition").alias("_kafka_partition"), "records_written"
        )
        .groupBy(*position)
        .agg(F.sum("records_written").alias("expected"))
    )
    actual = table.groupBy(*position).agg(F.count(F.lit(1)).alias("actual"))
    mismatched = (
        expected.join(actual, position, "full_outer")
        .where(~F.coalesce(F.col("expected"), F.lit(0)).eqNullSafe(F.coalesce(F.col("actual"), F.lit(0))))
        .orderBy(*position)
        .collect()
    )
    detail = "; ".join(
        f"{r._kafka_topic}[{r._kafka_partition}]: batches {r.expected}, table {r.actual}" for r in mismatched[:5]
    )
    return CheckResult("matches_change_batches", name, not mismatched, detail or "every partition reconciles")


def _pipeline_run_complete(spark: SparkSession, audit_namespace: str, pipeline: PipelineRun) -> CheckResult:
    spec = ingestion_run_table(audit_namespace)
    expected = {source for feed in pipeline.feeds for source in sources_of(feed)}
    succeeded: set[str] = set()
    if spark.catalog.tableExists(spec.identifier):
        rows = (
            spark.table(spec.identifier)
            .where(F.col("run_id").startswith(f"{pipeline.run_id}#") & (F.col("status") == "SUCCEEDED"))
            .select("source")
            .distinct()
            .collect()
        )
        succeeded = {r.source for r in rows}
    missing = sorted(expected - succeeded)
    detail = (
        f"no successful ingestion of {', '.join(missing)} by {pipeline.run_id} on this branch"
        if missing
        else f"{len(expected)} sources ingested by {pipeline.run_id}"
    )
    return CheckResult("pipeline_run_complete", f"{audit_namespace}.{spec.name}", not missing, detail)


def _business_date_delivered(registry: DataFrame, audit_namespace: str, pipeline: PipelineRun) -> list[CheckResult]:
    """Each dated feed holds an accepted file of the run's date, and no file the run rejected came after it.

    A corrected redelivery picked up when the failed run is resumed is accepted after the
    rejection, which clears it; a rejected redelivery of an accepted date still needs a look.
    """
    table = f"{audit_namespace}.ingestion_file"
    results = []
    for feed in (f for f in pipeline.feeds if f in DATED_FEEDS):
        files = registry.where(F.col("source").isin(*sources_of(feed)))
        last_accepted = (
            files.where(
                (F.col("business_date") == F.lit(pipeline.business_date)) & (F.col("status") == Status.ACCEPTED.value)
            )
            .agg(F.max("processed_at"))
            .collect()[0][0]
        )
        rejected = files.where(
            F.col("run_id").startswith(f"{pipeline.run_id}#") & (F.col("status") == Status.REJECTED.value)
        )
        if last_accepted is not None:
            rejected = rejected.where(F.col("processed_at") > F.lit(last_accepted))
        unresolved = rejected.select("file_name", "reason").distinct().orderBy("file_name").collect()
        if unresolved:
            detail = "; ".join(f"{r.file_name} rejected: {r.reason}" for r in unresolved)
        elif last_accepted is None:
            detail = f"no accepted {feed} file for {pipeline.business_date}"
        else:
            detail = f"{feed} file of {pipeline.business_date} accepted"
        passed = last_accepted is not None and not unresolved
        results.append(CheckResult(f"business_date_delivered:{feed}", table, passed, detail))
    return results


def run_checks(
    spark: SparkSession,
    run_id: str,
    bronze_namespace: str,
    audit_namespace: str,
    pipeline: PipelineRun | None = None,
) -> list[CheckResult]:
    results: list[CheckResult] = []
    if pipeline is not None:
        results.append(_pipeline_run_complete(spark, audit_namespace, pipeline))
    registry_spec = ingestion_file_table(audit_namespace)
    if not spark.catalog.tableExists(registry_spec.identifier):
        return _record(spark, run_id, audit_namespace, results)
    registry = spark.table(registry_spec.identifier).cache()
    if pipeline is not None and pipeline.business_date is not None:
        results += _business_date_delivered(registry, audit_namespace, pipeline)
    for spec in bronze_tables():
        name = f"{bronze_namespace}.{spec.name}"
        identifier = f"{CATALOG}.{name}"
        if not spark.catalog.tableExists(identifier):
            continue
        table = spark.table(identifier).cache()
        results += [_unique_key(table, spec, name), _matches_registry(table, registry, spec, name)]
        table.unpersist()

    batches_spec = corebanking.change_batch_table(audit_namespace)
    if spark.catalog.tableExists(batches_spec.identifier):
        batches = spark.table(batches_spec.identifier).cache()
        for table_name in change_log_tables():
            name = f"{bronze_namespace}.{table_name}"
            identifier = f"{CATALOG}.{name}"
            if not spark.catalog.tableExists(identifier):
                continue
            table = spark.table(identifier).cache()
            # Reading an offset twice would duplicate a change; the position identifies each event.
            position = BronzeTable(table_name, ("_kafka_topic", "_kafka_partition", "_kafka_offset"), False)
            results += [_unique_key(table, position, name), _matches_change_batches(table, batches, name)]
            table.unpersist()
    return _record(spark, run_id, audit_namespace, results)


def _record(spark: SparkSession, run_id: str, audit_namespace: str, results: list[CheckResult]) -> list[CheckResult]:
    target = publish_check_table(audit_namespace)
    ensure_table(spark, target)
    checked_at = utcnow()
    rows = [(run_id, r.check, r.table, r.passed, r.detail, checked_at) for r in results]
    if rows:
        spark.createDataFrame(rows, target.columns).writeTo(target.identifier).append()
    for result in results:
        log.log(
            logging.INFO if result.passed else logging.ERROR,
            "%s %s on %s: %s",
            "passed" if result.passed else "FAILED",
            result.check,
            result.table,
            result.detail,
        )
    return results
