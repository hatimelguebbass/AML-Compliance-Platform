"""What every ingestion job needs to know about the run it belongs to."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

from aml_platform.contracts import Contract
from aml_platform.ingestion.audit import FileOutcome, RunRecorder, Status
from aml_platform.ingestion.files import FileRejectedError, LandedFile, Rejection, landed, quarantine

# Lineage columns added to every bronze row. Leading underscores keep them apart from source fields.
LINEAGE_COLUMNS = "_source_file STRING, _ingest_run_id STRING, _ingested_at TIMESTAMP"

log = logging.getLogger(__name__)


class PendingDeliveryError(Exception):
    """The file is not complete yet (no completeness signal); try again on the next run."""


@dataclass(frozen=True)
class RunContext:
    spark: SparkSession
    run_id: str
    started_at: datetime
    landing_dir: Path
    quarantine_dir: Path
    bronze_namespace: str = "bronze"
    audit_namespace: str = "audit"
    # Restricts the dated file feeds to one business date; None takes every landed file.
    business_date: date | None = None

    def lineage(self, file: LandedFile) -> list[Column]:
        return [
            F.lit(file.name).alias("_source_file"),
            F.lit(self.run_id).alias("_ingest_run_id"),
            F.lit(self.started_at).cast("timestamp").alias("_ingested_at"),
        ]

    def run_feed(
        self,
        source: str,
        contract: Contract,
        target_tables: list[str],
        paths: Iterable[Path],
        ingest_file: Callable[[RunRecorder, LandedFile], None],
    ) -> RunRecorder:
        """Process each new file in order; a contract breach rejects that file only, anything else fails the run."""
        recorder = RunRecorder(self.spark, self.audit_namespace, self.run_id, source, self.started_at)
        try:
            processed = recorder.processed()
            for path in paths:
                file = landed(path)
                if (file.name, file.sha256) in processed:
                    recorder.files_skipped += 1
                    continue
                try:
                    ingest_file(recorder, file)
                except PendingDeliveryError as pending:
                    recorder.files_pending += 1
                    log.info("%s not ingested yet: %s", file.name, pending)
                except FileRejectedError as rejected:
                    self.reject(
                        recorder,
                        contract,
                        file,
                        target_tables,
                        str(rejected),
                        business_date=rejected.business_date,
                        records_in_file=rejected.records_in_file,
                    )
                finally:
                    self.spark.catalog.clearCache()
        except BaseException as error:
            recorder.finish(error)
            raise
        recorder.finish()
        return recorder

    def reject(
        self,
        recorder: RunRecorder,
        contract: Contract,
        file: LandedFile,
        target_tables: list[str],
        reason: str,
        *,
        business_date: date | None = None,
        records_in_file: int | None = None,
    ) -> None:
        quarantine(
            file,
            Rejection(
                file_name=file.name,
                sha256=file.sha256,
                source=recorder.source,
                contract_id=contract.id,
                run_id=self.run_id,
                reason=reason,
                rejected_at=self.started_at.isoformat(),
            ),
            self.quarantine_dir,
        )
        for table in target_tables:
            recorder.record(
                FileOutcome(
                    contract_id=contract.id,
                    contract_version=contract.version,
                    file_name=file.name,
                    file_sha256=file.sha256,
                    target_table=table,
                    business_date=business_date,
                    status=Status.REJECTED,
                    reason=reason,
                    records_in_file=records_in_file,
                )
            )
