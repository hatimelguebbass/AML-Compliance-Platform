"""Core banking change events from Kafka into append-only bronze change logs.

Each table of the core banking database has a Debezium topic and a bronze table
holding every captured version of its rows: the initial snapshot (``r``), then
creates, updates and deletes in commit order per row. Silver derives current
state and history from it.

A run reads every partition from the offset after the last one stored in the
bronze table up to the end of the topic at the start of the run. Offsets are
columns of the rows, so data and position move in the same Iceberg commit: no
crash can separate them, and a staging branch that is thrown away rewinds the
position too, exactly like the file registry of the batch feeds.

A change event cannot be set aside like a file: later events of the same row
build on it. Any contract breach stops the run before anything is written, and
the topic keeps the events until the cause is fixed.
"""

from __future__ import annotations

import json
import logging

from pyspark.sql import Column, DataFrame, Row
from pyspark.sql import functions as F

from aml_platform import contracts
from aml_platform.cdc.topics import CHANGE_TOPIC_PARTITIONS, CdcSource
from aml_platform.contracts import Property, SchemaObject
from aml_platform.ingestion.audit import RunRecorder, utcnow
from aml_platform.ingestion.context import RunContext
from aml_platform.ingestion.schema import is_instant, quote, spark_type, struct_ddl
from aml_platform.ingestion.tables import TableSpec, ensure_table

SOURCE = "corebanking"
CONTRACT_ID = "corebanking-customer-master"
# Snapshot read, create, update, delete. Truncates are not captured (Debezium skips them by default).
OPERATIONS = ("r", "c", "u", "d")
# Debezium's stand-in for a TOASTed value that an update did not change and the WAL does not carry.
UNAVAILABLE = "__debezium_unavailable_value"
EARLIEST = -2

CHANGE_COLUMNS = (
    "_op STRING, _snapshot STRING, _source_lsn BIGINT, _source_tx_id BIGINT, _source_committed_at TIMESTAMP,"
    " _kafka_topic STRING, _kafka_partition INT, _kafka_offset BIGINT, _ingest_run_id STRING, _ingested_at TIMESTAMP"
)

log = logging.getLogger(__name__)


class ChangeEventContractError(RuntimeError):
    """Change events that do not match the contract; nothing of the topic was written."""


def table_name(obj: SchemaObject) -> str:
    return f"corebanking_{obj.name}"


def table_spec(namespace: str, contract: contracts.Contract, obj: SchemaObject) -> TableSpec:
    return TableSpec(
        namespace=namespace,
        name=table_name(obj),
        columns=f"{struct_ddl(obj.properties)}, {CHANGE_COLUMNS}",
        comment=f"Every captured version of core.{obj.name} rows (contract {contract.id} {contract.version})",
    )


def change_batch_table(namespace: str) -> TableSpec:
    return TableSpec(
        namespace=namespace,
        name="change_batch",
        columns=(
            "run_id STRING, source STRING, contract_id STRING, contract_version STRING, topic STRING,"
            " kafka_partition INT, target_table STRING, start_offset BIGINT, end_offset BIGINT,"
            " events_read BIGINT, tombstones BIGINT, records_written BIGINT, processed_at TIMESTAMP,"
            " business_date DATE"
        ),
        comment="One row per topic partition read by a change data capture run",
    )


def wire_type(prop: Property) -> str:
    """Spark type of a column as Debezium's JSON converter encodes it."""
    if prop.logical_type == "date":
        return "INT"  # days since the epoch
    if prop.logical_type == "timestamp":
        return "STRING" if is_instant(prop) else "BIGINT"  # ISO-8601 with zone, or microseconds since the epoch
    if prop.logical_type == "number":
        return "STRING"  # decimal.handling.mode=string keeps the exact value
    return spark_type(prop)


def from_wire(value: Column, prop: Property) -> Column:
    if prop.logical_type == "date":
        return F.date_from_unix_date(value)
    if prop.logical_type == "timestamp":
        return value.try_cast("timestamp") if is_instant(prop) else F.timestamp_micros(value).cast("timestamp_ntz")
    if prop.logical_type == "number":
        return value.try_cast(spark_type(prop))
    return value


def envelope_ddl(obj: SchemaObject) -> str:
    image = f"STRUCT<{', '.join(f'{quote(p.name)}: {wire_type(p)}' for p in obj.properties)}>"
    source = "STRUCT<snapshot: STRING, lsn: BIGINT, txId: BIGINT, ts_ms: BIGINT, schema: STRING, table: STRING>"
    return f"before {image}, after {image}, op STRING, source {source}"


def starting_offsets(topic: str, stored: dict[int, int]) -> str:
    """Spark ``startingOffsets`` for every declared partition: after the last stored offset, else the earliest."""
    return json.dumps(
        {topic: {str(p): stored[p] + 1 if p in stored else EARLIEST for p in range(CHANGE_TOPIC_PARTITIONS)}}
    )


def _violations(events: DataFrame, source: CdcSource, obj: SchemaObject) -> dict[str, int]:
    expected_fields = F.array(*(F.lit(name) for name in sorted(obj.column_names)))
    counters = [
        F.count_if(F.col("_op").isNull()).alias("unparseable"),
        F.count_if(~F.col("_op").isin(*OPERATIONS)).alias("unknown_operation"),
        F.count_if((F.col("__schema") != source.schema) | (F.col("__table") != obj.name)).alias("wrong_table"),
        F.count_if(
            ~F.coalesce(F.array_sort(F.json_object_keys(F.col("__image"))) == expected_fields, F.lit(False))
        ).alias("fields_differ_from_contract"),
    ]
    for prop in obj.properties:
        raw = F.get_json_object(F.col("__image"), f"$.{prop.name}")
        counters.append(F.count_if(raw.isNotNull() & F.col(prop.name).isNull()).alias(f"{prop.name}:wrong_type"))
        # Captured tables have REPLICA IDENTITY FULL: a delete carries the whole row, like any other event.
        if prop.required or prop.primary_key:
            counters.append(F.count_if(F.col(prop.name).isNull()).alias(f"{prop.name}:missing"))
        if spark_type(prop) == "STRING":
            counters.append(F.count_if(F.col(prop.name) == UNAVAILABLE).alias(f"{prop.name}:unavailable"))
    row = events.agg(*counters).collect()[0]
    return {name: count for name, count in row.asDict().items() if count}


def _ingest_topic(
    ctx: RunContext,
    source: CdcSource,
    contract: contracts.Contract,
    obj: SchemaObject,
    bootstrap_servers: str,
    batches: TableSpec,
) -> int:
    spark = ctx.spark
    spec = table_spec(ctx.bronze_namespace, contract, obj)
    ensure_table(spark, spec)
    topic = source.change_topic(obj.name)

    stored = {
        r._kafka_partition: r.last
        for r in spark.table(spec.identifier)
        .where(F.col("_kafka_topic") == topic)
        .groupBy("_kafka_partition")
        .agg(F.max("_kafka_offset").alias("last"))
        .collect()
    }
    # "latest" is resolved each time the plan runs; materialising the read pins the end offsets, so the
    # checks and the write see the same events even while the connector keeps producing.
    read = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets(topic, stored))
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "true")
        .load()
        .select("partition", "offset", F.col("value").cast("string").alias("value"))
        .localCheckpoint(eager=True)
    )
    ranges = (
        read.groupBy("partition")
        .agg(
            F.min("offset").alias("first"),
            F.max("offset").alias("last"),
            F.count(F.lit(1)).alias("events"),
            F.count_if(F.col("value").isNull()).alias("tombstones"),
        )
        .orderBy("partition")
        .collect()
    )
    if not ranges:
        log.info("%s: no new change events", topic)
        return 0
    for r in ranges:
        if r.partition in stored and r.first > stored[r.partition] + 1:
            # Expected on a compacted topic only if this consumer fell behind by more than min.compaction.lag.ms.
            log.warning(
                "%s[%s]: offsets %s to %s no longer in the topic",
                topic,
                r.partition,
                stored[r.partition] + 1,
                r.first - 1,
            )

    envelope = F.from_json(F.col("value"), envelope_ddl(obj))
    is_delete = envelope["op"] == "d"
    image = F.when(is_delete, envelope["before"]).otherwise(envelope["after"])
    events = read.where(F.col("value").isNotNull()).select(
        *(from_wire(image[p.name], p).alias(p.name) for p in obj.properties),
        envelope["op"].alias("_op"),
        envelope["source"]["snapshot"].alias("_snapshot"),
        envelope["source"]["lsn"].alias("_source_lsn"),
        envelope["source"]["txId"].alias("_source_tx_id"),
        F.timestamp_millis(envelope["source"]["ts_ms"]).alias("_source_committed_at"),
        F.lit(topic).alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.lit(ctx.run_id).alias("_ingest_run_id"),
        F.lit(ctx.started_at).cast("timestamp").alias("_ingested_at"),
        F.when(is_delete, F.get_json_object("value", "$.before"))
        .otherwise(F.get_json_object("value", "$.after"))
        .alias("__image"),
        envelope["source"]["schema"].alias("__schema"),
        envelope["source"]["table"].alias("__table"),
    )

    violations = _violations(events, source, obj)
    if violations:
        span = ", ".join(f"[{r.partition}] {r.first}-{r.last}" for r in ranges)
        detail = ", ".join(f"{name}={count}" for name, count in sorted(violations.items()))
        raise ChangeEventContractError(f"{topic} offsets {span}: {detail}")

    events.drop("__image", "__schema", "__table").writeTo(spec.identifier).append()

    rows = [
        Row(
            run_id=ctx.run_id,
            source=SOURCE,
            contract_id=contract.id,
            contract_version=contract.version,
            topic=topic,
            kafka_partition=r.partition,
            target_table=spec.name_in_catalog,
            start_offset=r.first,
            end_offset=r.last + 1,
            events_read=r.events,
            tombstones=r.tombstones,
            records_written=r.events - r.tombstones,
            processed_at=utcnow(),
            business_date=ctx.business_date,
        )
        for r in ranges
    ]
    spark.createDataFrame(rows, batches.columns).writeTo(batches.identifier).append()
    written = int(sum(r.events - r.tombstones for r in ranges))
    log.info("%s -> %s: %s change events written", topic, spec.name_in_catalog, written)
    return written


def ingest(ctx: RunContext, source: CdcSource, bootstrap_servers: str) -> RunRecorder:
    contract = contracts.load_by_id(CONTRACT_ID)
    recorder = RunRecorder(ctx.spark, ctx.audit_namespace, ctx.run_id, SOURCE, ctx.started_at)
    batches = change_batch_table(ctx.audit_namespace)
    ensure_table(ctx.spark, batches)
    try:
        for table in source.tables:
            recorder.events_written += _ingest_topic(
                ctx, source, contract, contract.object(table), bootstrap_servers, batches
            )
            ctx.spark.catalog.clearCache()
    except BaseException as error:
        recorder.finish(error)
        raise
    recorder.finish()
    return recorder
