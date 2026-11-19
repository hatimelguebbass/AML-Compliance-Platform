"""Payments hub files into ``bronze.payment_transfer``, one partition per business date.

A file is read only once its manifest is delivered and agrees with it (name,
SHA-256, record count). A corrected redelivery of a business date replaces that
date's partition in a single commit, so re-running is idempotent.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from aml_platform import contracts
from aml_platform.ingestion.audit import FileOutcome, RunRecorder, Status
from aml_platform.ingestion.context import LINEAGE_COLUMNS, PendingDeliveryError, RunContext
from aml_platform.ingestion.files import (
    PAYMENTS_FILE,
    FileRejectedError,
    LandedFile,
    check_csv_header,
    data_files,
    payments_business_date,
    verified_manifest,
)
from aml_platform.ingestion.schema import quote, struct_ddl
from aml_platform.ingestion.tables import TableSpec, ensure_table
from aml_platform.ingestion.validation import MALFORMED, validate

SOURCE = "payments"
CONTRACT_ID = "payments-hub-transfers"
OBJECT = "payment_transfer"
TABLE = "payment_transfer"
_CORRUPT = "_corrupt_record"


def table_spec(ctx: RunContext, contract: contracts.Contract) -> TableSpec:
    obj = contract.object(OBJECT)
    return TableSpec(
        namespace=ctx.bronze_namespace,
        name=TABLE,
        columns=f"{struct_ddl(obj.properties)}, _business_date DATE, {LINEAGE_COLUMNS}",
        partitioned_by="_business_date",
        comment=f"Payments hub transfers as delivered (contract {contract.id} {contract.version})",
    )


def ingest(ctx: RunContext) -> RunRecorder:
    contract = contracts.load_by_id(CONTRACT_ID)
    obj = contract.object(OBJECT)
    spec = table_spec(ctx, contract)
    ensure_table(ctx.spark, spec)
    raw_schema = ", ".join([f"{quote(name)} STRING" for name in obj.column_names] + [f"{_CORRUPT} STRING"])

    def ingest_file(recorder: RunRecorder, file: LandedFile) -> None:
        business_date = payments_business_date(file.path)
        try:
            manifest = verified_manifest(file)
        except FileNotFoundError:
            raise PendingDeliveryError("manifest not delivered") from None
        check_csv_header(file.path, obj.column_names)

        raw = (
            ctx.spark.read.schema(raw_schema)
            .options(header=True, mode="PERMISSIVE", columnNameOfCorruptRecord=_CORRUPT, encoding="UTF-8")
            .csv(file.path.as_uri())
        )
        validated = validate(raw.withColumn(MALFORMED, F.col(_CORRUPT).isNotNull()).drop(_CORRUPT), obj)
        problems = dict(validated.violations)
        if validated.record_count != manifest.record_count:
            problems["record_count_matches_manifest"] = validated.record_count
        if problems:
            raise FileRejectedError(
                ", ".join(f"{name}={count}" for name, count in sorted(problems.items())),
                business_date=business_date,
                records_in_file=validated.record_count,
            )

        validated.records.select(
            *obj.column_names, F.lit(business_date).alias("_business_date"), *ctx.lineage(file)
        ).writeTo(spec.identifier).overwrite(F.col("_business_date") == F.lit(business_date))
        recorder.record(
            FileOutcome(
                contract_id=contract.id,
                contract_version=contract.version,
                file_name=file.name,
                file_sha256=file.sha256,
                target_table=spec.name_in_catalog,
                business_date=business_date,
                status=Status.ACCEPTED,
                records_in_file=validated.record_count,
                records_written=validated.record_count,
            )
        )

    paths = data_files(ctx.landing_dir / "payments", PAYMENTS_FILE, ctx.business_date)
    return ctx.run_feed(SOURCE, contract, [spec.name_in_catalog], paths, ingest_file)
