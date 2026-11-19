"""``aml-reporting``: check the Power BI project, and compute the figures its report must show."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import trino
from trino.auth import BasicAuthentication

from aml_platform.reporting import figures, pbip, schemas

log = logging.getLogger("aml_platform.reporting")


def _report(found: list[str], passed: str) -> int:
    for problem in found:
        log.error("%s", problem)
    log.info("%s", f"{len(found)} problems" if found else passed)
    return 1 if found else 0


def _check(args: argparse.Namespace) -> int:
    return _report(pbip.problems(args.project, args.models), "report, semantic model and dbt contracts agree")


def _validate(args: argparse.Namespace) -> int:
    return _report(schemas.validate(args.project), "every Power BI project file matches the schema it declares")


def _figures(args: argparse.Namespace) -> int:
    password = os.environ.get("TRINO_BI_PASSWORD")
    if not password:
        log.error("TRINO_BI_PASSWORD is not set")
        return 2
    # The same path as Power BI: the HTTPS listener, the bi user and its password.
    connection = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host=args.trino_host,
        port=args.trino_port,
        user="bi",
        auth=BasicAuthentication("bi", password),
        http_scheme="https",
        verify=str(args.ca_cert),
        catalog="lakehouse",
    )
    period = f"{args.first or 'first'} to {args.last or 'last'} business date"
    log.info("figures the report must show, %s", period)
    for figure in figures.compute(connection.cursor(), args.first, args.last):
        if isinstance(figure.value, Decimal):
            value = f"{figure.value:,.2f}"
        elif isinstance(figure.value, int):
            value = f"{figure.value:,}"
        else:
            value = str(figure.value)
        log.info("%-20s %-28s %s", figure.page, figure.measure, value)
    return 0


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="aml-reporting", description=__doc__)
    parser.add_argument("--project", type=Path, default=Path("bi"), help="the Power BI project folder")
    parser.add_argument("--models", type=Path, default=Path("transform/models"), help="the dbt models folder")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("check", help="report fields, DAX references and source columns against the dbt contracts")
    commands.add_parser("validate", help="validate the project files against Microsoft's published schemas")
    figures_parser = commands.add_parser("figures", help="the figures of the report's cards, over HTTPS as bi")
    figures_parser.add_argument("--trino-host", default="localhost")
    figures_parser.add_argument("--trino-port", type=int, default=8443)
    figures_parser.add_argument("--ca-cert", type=Path, default=Path("data/trino.crt"))
    figures_parser.add_argument("--from", dest="first", type=date.fromisoformat)
    figures_parser.add_argument("--until", dest="last", type=date.fromisoformat)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for noisy in ("trino", "httpx"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    sys.exit({"check": _check, "validate": _validate, "figures": _figures}[args.command](args))
