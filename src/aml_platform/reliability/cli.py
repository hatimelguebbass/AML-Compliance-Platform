"""``aml-reliability``: rebuild the derived layers on the staging branch and compare them with main."""

from __future__ import annotations

import argparse
import logging
import os
import sys

import trino

from aml_platform.reliability import regeneration

log = logging.getLogger("aml_platform.reliability")


def _cursor(args: argparse.Namespace) -> regeneration.Cursor:
    # The client's connect() is unannotated.
    connection = trino.dbapi.connect(host=args.trino_host, port=args.trino_port, user=args.trino_user)  # type: ignore[no-untyped-call]
    cursor: regeneration.Cursor = connection.cursor()
    return cursor


def _drop(args: argparse.Namespace) -> int:
    cursor = _cursor(args)
    for statement in regeneration.drop_statements(cursor):
        log.info("%s", statement)
        cursor.execute(statement)
        cursor.fetchall()
    return 0


def _compare(args: argparse.Namespace) -> int:
    results = regeneration.compare(_cursor(args))
    for result in results:
        rows = " / ".join("-" if side is None else f"{side.rows:,}" for side in (result.published, result.rebuilt))
        log.log(
            logging.INFO if result.matches else logging.ERROR,
            "%-45s %-18s rows %s",
            f"{result.namespace}.{result.table}",
            result.verdict,
            rows,
        )
    differing = [r for r in results if not r.matches]
    log.info("%s tables compared, %s identical, %s differ", len(results), len(results) - len(differing), len(differing))
    return 1 if differing or not results else 0


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="aml-reliability", description=__doc__)
    parser.add_argument("--trino-host", default=os.environ.get("TRINO_HOST", "localhost"))
    parser.add_argument("--trino-port", type=int, default=int(os.environ.get("TRINO_PORT", "8080")))
    parser.add_argument("--trino-user", default="trino")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("drop-derived", help="drop the derived tables of the staging branch, before a rebuild")
    commands.add_parser("compare", help="compare the derived tables of the staging branch with main")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("trino").setLevel(logging.WARNING)
    sys.exit({"drop-derived": _drop, "compare": _compare}[args.command](args))
