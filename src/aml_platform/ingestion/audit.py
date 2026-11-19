"""Run metadata in the audit namespace: one row per run, one per file and target table.

The file rows double as the ingestion registry: a file whose checksum is already
recorded as accepted or rejected on the branch is not processed again. Because
the registry lives on the same branch as the data, a staging run that is thrown
away also forgets which files it processed, and they are picked up again.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from aml_platform.ingestion.tables import TableSpec, ensure_table

log = logging.getLogger(__name__)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ingestion_run_table(namespace: str) -> TableSpec:
    return TableSpec(
        namespace=namespace,
        name="ingestion_run",
        columns=(
            "run_id STRING, source STRING, started_at TIMESTAMP, finished_at TIMESTAMP, status STRING,"
            " files_accepted INT, files_rejected INT, files_skipped INT, files_pending INT,"
            " records_written BIGINT, error STRING"
        ),
        comment="One row per ingestion run of a source",
    )


def ingestion_file_table(namespace: str) -> TableSpec:
    return TableSpec(
        namespace=namespace,
        name="ingestion_file",
        columns=(
            "run_id STRING, source STRING, contract_id STRING, contract_version STRING,"
            " file_name STRING, file_sha256 STRING, target_table STRING, business_date DATE,"
            " status STRING, reason STRING,"
            " records_in_file BIGINT, records_written BIGINT, duplicates_dropped BIGINT, already_present BIGINT,"
            " processed_at TIMESTAMP"
        ),
        comment="One row per landed file and target table processed by ingestion; also the ingestion registry",
    )


class Status(str, Enum):
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class FileOutcome:
    contract_id: str
    contract_version: str
    file_name: str
    file_sha256: str
    target_table: str
    business_date: date | None
    status: Status
    reason: str | None = None
    records_in_file: int | None = None
    records_written: int = 0
    duplicates_dropped: int = 0
    already_present: int = 0


@dataclass
class RunRecorder:
    spark: SparkSession
    namespace: str
    run_id: str
    source: str
    started_at: datetime
    files_skipped: int = 0
    files_pending: int = 0
    # Records written by sources that deliver change events rather than files.
    events_written: int = 0
    outcomes: list[FileOutcome] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._runs = ingestion_run_table(self.namespace)
        self._files = ingestion_file_table(self.namespace)
        ensure_table(self.spark, self._runs)
        ensure_table(self.spark, self._files)

    def processed(self) -> set[tuple[str, str]]:
        """``(file_name, sha256)`` of every file of this source already accepted or rejected."""
        rows = (
            self.spark.table(self._files.identifier)
            .where(F.col("source") == self.source)
            .select("file_name", "file_sha256")
            .distinct()
            .collect()
        )
        return {(r.file_name, r.file_sha256) for r in rows}

    def record(self, outcome: FileOutcome) -> None:
        """Persist one outcome right after its data commit, so the registry never lags the data."""
        self.outcomes.append(outcome)
        row = Row(
            run_id=self.run_id,
            source=self.source,
            contract_id=outcome.contract_id,
            contract_version=outcome.contract_version,
            file_name=outcome.file_name,
            file_sha256=outcome.file_sha256,
            target_table=outcome.target_table,
            business_date=outcome.business_date,
            status=outcome.status.value,
            reason=outcome.reason,
            records_in_file=outcome.records_in_file,
            records_written=outcome.records_written,
            duplicates_dropped=outcome.duplicates_dropped,
            already_present=outcome.already_present,
            processed_at=utcnow(),
        )
        self.spark.createDataFrame([row], self._files.columns).writeTo(self._files.identifier).append()
        level = logging.INFO if outcome.status is Status.ACCEPTED else logging.WARNING
        log.log(
            level,
            "%s %s -> %s: %s; %s records written%s",
            outcome.status.value.lower(),
            outcome.file_name,
            outcome.target_table,
            outcome.reason or "contract checks passed",
            outcome.records_written,
            f", {outcome.duplicates_dropped} resent records dropped" if outcome.duplicates_dropped else "",
        )

    def finish(self, error: BaseException | None = None) -> None:
        accepted = {o.file_name for o in self.outcomes if o.status is Status.ACCEPTED}
        rejected = {o.file_name for o in self.outcomes if o.status is Status.REJECTED}
        row = Row(
            run_id=self.run_id,
            source=self.source,
            started_at=self.started_at,
            finished_at=utcnow(),
            status="FAILED" if error else "SUCCEEDED",
            files_accepted=len(accepted),
            files_rejected=len(rejected),
            files_skipped=self.files_skipped,
            files_pending=self.files_pending,
            records_written=sum(o.records_written for o in self.outcomes) + self.events_written,
            error=repr(error)[:2000] if error else None,
        )
        self.spark.createDataFrame([row], self._runs.columns).writeTo(self._runs.identifier).append()
        log.info(
            "%s run %s: %s files accepted, %s rejected, %s already ingested, %s awaiting their manifest;"
            " %s records written",
            self.source,
            row.status.lower(),
            row.files_accepted,
            row.files_rejected,
            row.files_skipped,
            row.files_pending,
            row.records_written,
        )
