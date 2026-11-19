"""Command line entry point: ``aml-cdc <command>``."""

from __future__ import annotations

import argparse
import logging
import os

from aml_platform.cdc import connect, kafka_admin, source_db
from aml_platform.cdc.topics import CORE_BANKING
from aml_platform.sources.cli import source_db_conninfo

log = logging.getLogger("aml_platform.cdc")

OPERATOR_ERRORS: tuple[type[Exception], ...] = (
    connect.ConnectorFailedError,
    kafka_admin.TopicMismatchError,
    source_db.SourceNotReadyError,
)


def _setup(args: argparse.Namespace) -> None:
    source_db.prepare(
        source_db_conninfo(), CORE_BANKING, role=os.environ["CDC_DB_USER"], password=os.environ["CDC_DB_PASSWORD"]
    )
    kafka_admin.ensure_topics(CORE_BANKING.topics())
    with connect.KafkaConnect(args.connect_url) as client:
        client.apply(CORE_BANKING.connector_name, connect.connector_config(CORE_BANKING))
        client.wait_until_running(CORE_BANKING.connector_name)


def _reset(args: argparse.Namespace) -> None:
    with connect.KafkaConnect(args.connect_url) as client:
        client.remove(CORE_BANKING.connector_name)
    source_db.drop(source_db_conninfo(), CORE_BANKING)
    kafka_admin.delete_topics([topic.name for topic in CORE_BANKING.topics()])
    log.info("capture removed: run setup again for a new snapshot")


def _status(args: argparse.Namespace) -> None:
    with connect.KafkaConnect(args.connect_url) as client:
        status = client.status(CORE_BANKING.connector_name)
    log.info(
        "connector %s: %s, tasks %s",
        status["name"],
        status["connector"]["state"],
        ", ".join(task["state"] for task in status["tasks"]) or "none",
    )
    for table in CORE_BANKING.tables:
        topic = CORE_BANKING.change_topic(table)
        offsets = kafka_admin.end_offsets(topic)
        log.info("%s: %s events", topic, sum(offsets.values()))
    lag = source_db.slot_lag_bytes(source_db_conninfo(), CORE_BANKING)
    log.info("replication slot %s: %s", CORE_BANKING.slot_name, "missing" if lag is None else f"{lag} bytes behind")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aml-cdc", description="Operate change data capture of the core banking.")
    parser.add_argument("--connect-url", default=os.environ.get("CONNECT_URL", "http://localhost:8083"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "setup", help="capture role and publication, topics, then the Debezium connector (idempotent)"
    ).set_defaults(handler=_setup)
    commands.add_parser(
        "reset", help="remove connector, offsets, slot, publication and topics; the next setup takes a new snapshot"
    ).set_defaults(handler=_reset)
    commands.add_parser("status", help="connector state, events per topic, replication slot lag").set_defaults(
        handler=_status
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        args.handler(args)
    except OPERATOR_ERRORS as error:
        log.error("%s", error)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
