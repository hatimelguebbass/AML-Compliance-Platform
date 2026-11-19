"""Entry point of the Spark ingestion jobs: ``spark-submit jobs/ingest.py <command>``."""

from __future__ import annotations

import argparse
import logging
import uuid
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from aml_platform import contracts
from aml_platform.cdc.topics import CORE_BANKING
from aml_platform.ingestion import cards, corebanking, payments, sanctions
from aml_platform.ingestion.audit import ingestion_file_table, ingestion_run_table
from aml_platform.ingestion.checks import PipelineRun, publish_check_table, run_checks
from aml_platform.ingestion.context import RunContext
from aml_platform.ingestion.tables import TableSpec, ensure_table

log = logging.getLogger("aml_platform.ingestion")

# "files" is every landed-file feed; "all" adds the core banking change events. "tables" creates
# the bronze and audit tables, empty, where absent: silver then builds before the first delivery.
COMMANDS = ("payments", "cards", "sanctions", "files", "corebanking", "all", "audit", "tables")
FILE_FEEDS = ("payments", "cards", "sanctions")
FEEDS = (*FILE_FEEDS, "corebanking")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="ingest", description=__doc__)
    parser.add_argument("command", choices=COMMANDS)
    parser.add_argument("--run-id", default=None, help="identifier recorded in the audit tables (default: random)")
    parser.add_argument(
        "--business-date",
        type=date.fromisoformat,
        default=None,
        help=(
            "only the payments and card files of this date (YYYY-MM-DD); for change events, the business date"
            " their capture closes, recorded in the change batches; sanctions snapshots are not dated;"
            " audit: the date the pipeline run publishes, which must hold an accepted file of each dated feed"
        ),
    )
    parser.add_argument(
        "--pipeline-run",
        default=None,
        help="audit: the orchestrator run whose ingestions (run ids <pipeline-run>#<attempt>) must be on the branch",
    )
    parser.add_argument(
        "--pipeline-feeds", nargs="+", choices=FEEDS, default=list(FEEDS), help="audit: the feeds that run ingests"
    )
    parser.add_argument("--landing-dir", type=Path, default=Path("/data/landing"))
    parser.add_argument("--quarantine-dir", type=Path, default=Path("/data/quarantine"))
    parser.add_argument("--bronze-namespace", default="bronze")
    parser.add_argument("--audit-namespace", default="audit")
    parser.add_argument("--kafka-bootstrap-servers", default="kafka:9092")
    parser.add_argument("--cdc-topic-prefix", default=CORE_BANKING.topic_prefix)
    parser.add_argument("--cdc-schema", default=CORE_BANKING.schema, help="captured core banking schema")
    return parser.parse_args(argv)


def table_specs(ctx: RunContext) -> list[TableSpec]:
    """Every table ingestion and audit write, as each job would create it."""
    sanctions_contract = contracts.load_by_id(sanctions.CONTRACT_ID)
    change_log = contracts.load_by_id(corebanking.CONTRACT_ID)
    return [
        payments.table_spec(ctx, contracts.load_by_id(payments.CONTRACT_ID)),
        cards.table_spec(ctx, contracts.load_by_id(cards.CONTRACT_ID)),
        *(sanctions.table_spec(ctx, sanctions_contract, obj) for obj in sanctions_contract.objects),
        *(corebanking.table_spec(ctx.bronze_namespace, change_log, obj) for obj in change_log.objects),
        corebanking.change_batch_table(ctx.audit_namespace),
        ingestion_run_table(ctx.audit_namespace),
        ingestion_file_table(ctx.audit_namespace),
        publish_check_table(ctx.audit_namespace),
    ]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run_id = args.run_id or uuid.uuid4().hex
    spark = SparkSession.builder.appName(f"ingest-{args.command}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.command == "audit":
            pipeline = (
                PipelineRun(args.pipeline_run, tuple(args.pipeline_feeds), args.business_date)
                if args.pipeline_run
                else None
            )
            results = run_checks(spark, run_id, args.bronze_namespace, args.audit_namespace, pipeline)
            failed = [r for r in results if not r.passed]
            if failed:
                log.error("%s of %s checks failed: the staging branch must not be published", len(failed), len(results))
                return 1
            log.info("%s checks passed", len(results))
            return 0

        ctx = RunContext(
            spark=spark,
            run_id=run_id,
            started_at=datetime.now(timezone.utc),
            landing_dir=args.landing_dir,
            quarantine_dir=args.quarantine_dir,
            bronze_namespace=args.bronze_namespace,
            audit_namespace=args.audit_namespace,
            business_date=args.business_date,
        )
        if args.command == "tables":
            for spec in table_specs(ctx):
                ensure_table(spark, spec)
            return 0
        feeds = {args.command} | (set(FILE_FEEDS) if args.command in ("files", "all") else set())
        if "payments" in feeds:
            payments.ingest(ctx)
        if "cards" in feeds:
            cards.ingest(ctx)
        if "sanctions" in feeds:
            sanctions.ingest(ctx)
        if args.command in ("corebanking", "all"):
            source = replace(CORE_BANKING, topic_prefix=args.cdc_topic_prefix, schema=args.cdc_schema)
            corebanking.ingest(ctx, source, args.kafka_bootstrap_servers)
        return 0
    finally:
        spark.stop()
