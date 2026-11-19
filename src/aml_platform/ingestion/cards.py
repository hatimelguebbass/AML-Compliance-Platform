"""Card processor files into ``bronze.card_transaction``.

A file is accepted as a whole or not at all: its header must match its name and
its trailer must agree with the detail records (count and amount total); a
missing trailer means the transfer was cut short. Records the processor sends
twice are dropped by ``txn_ref``, within a file and against earlier files, so a
re-run or a resent file adds nothing.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from aml_platform import contracts
from aml_platform.ingestion.audit import FileOutcome, RunRecorder, Status
from aml_platform.ingestion.context import LINEAGE_COLUMNS, RunContext
from aml_platform.ingestion.files import (
    CARD_DELIMITER,
    CARDS_FILE,
    FileRejectedError,
    LandedFile,
    card_control,
    data_files,
)
from aml_platform.ingestion.schema import struct_ddl
from aml_platform.ingestion.tables import TableSpec, ensure_table
from aml_platform.ingestion.validation import MALFORMED, validate

SOURCE = "cards"
CONTRACT_ID = "card-processor-transactions"
OBJECT = "card_transaction"
TABLE = "card_transaction"
KEY = "txn_ref"


def table_spec(ctx: RunContext, contract: contracts.Contract) -> TableSpec:
    obj = contract.object(OBJECT)
    return TableSpec(
        namespace=ctx.bronze_namespace,
        name=TABLE,
        columns=f"{struct_ddl(obj.properties)}, _business_date DATE, {LINEAGE_COLUMNS}",
        partitioned_by="_business_date",
        comment=f"Card processor detail records, resends removed (contract {contract.id} {contract.version})",
    )


def ingest(ctx: RunContext) -> RunRecorder:
    contract = contracts.load_by_id(CONTRACT_ID)
    obj = contract.object(OBJECT)
    spec = table_spec(ctx, contract)
    ensure_table(ctx.spark, spec)
    amount_position = obj.column_names.index("amount_minor")

    def ingest_file(recorder: RunRecorder, file: LandedFile) -> None:
        control = card_control(file.path)
        fields = F.split(F.col("value"), f"\\{CARD_DELIMITER}", -1)
        lines = ctx.spark.read.text(file.path.as_uri()).select(fields.alias("f"))
        record_type = F.col("f")[0]

        totals = lines.agg(
            F.count_if(record_type == "H").alias("headers"),
            F.count_if(record_type == "T").alias("trailers"),
            F.count_if(~record_type.isin("H", "D", "T")).alias("unknown"),
            F.count_if(record_type == "D").alias("details"),
            F.sum(F.when(record_type == "D", F.col("f")[amount_position].try_cast("bigint"))).alias("amount"),
        ).collect()[0]
        if (totals.headers, totals.trailers, totals.unknown) != (1, 1, 0):
            raise FileRejectedError(
                f"record types: {totals.headers} headers, {totals.trailers} trailers, {totals.unknown} unknown",
                business_date=control.business_date,
            )
        if (totals.details, totals.amount or 0) != (control.declared_count, control.declared_total):
            raise FileRejectedError(
                f"trailer declares {control.declared_count} records totalling {control.declared_total},"
                f" file holds {totals.details} totalling {totals.amount}",
                business_date=control.business_date,
                records_in_file=totals.details,
            )

        empty_to_null = [
            F.nullif(F.col("f")[position], F.lit("")).alias(name) for position, name in enumerate(obj.column_names)
        ]
        raw = lines.where(record_type == "D").select(
            *empty_to_null, (F.size("f") != len(obj.column_names)).alias(MALFORMED)
        )
        validated = validate(raw, obj)
        if validated.violations:
            raise FileRejectedError(
                validated.describe_violations(),
                business_date=control.business_date,
                records_in_file=validated.record_count,
            )

        existing = ctx.spark.table(spec.identifier).select(KEY)
        new = validated.records.join(existing, KEY, "left_anti").cache()
        written = new.count()
        new.select(*obj.column_names, F.lit(control.business_date).alias("_business_date"), *ctx.lineage(file)).writeTo(
            spec.identifier
        ).append()
        recorder.record(
            FileOutcome(
                contract_id=contract.id,
                contract_version=contract.version,
                file_name=file.name,
                file_sha256=file.sha256,
                target_table=spec.name_in_catalog,
                business_date=control.business_date,
                status=Status.ACCEPTED,
                records_in_file=validated.record_count,
                records_written=written,
                duplicates_dropped=validated.duplicates_dropped,
                already_present=validated.record_count - validated.duplicates_dropped - written,
            )
        )

    paths = data_files(ctx.landing_dir / "cards", CARDS_FILE, ctx.business_date)
    return ctx.run_feed(SOURCE, contract, [spec.name_in_catalog], paths, ingest_file)
