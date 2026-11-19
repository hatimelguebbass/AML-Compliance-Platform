"""``aml-evaluate``: score published outputs against the ground truth in data/ground_truth."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

import trino

from aml_platform.evaluation import parties, rules, sanctions

log = logging.getLogger("aml_platform.evaluation")

# Published data only (the main branch), read with the rights of who would read it: alerts and
# sanctions hits are confidential to compliance.
READERS = {"parties": "analyst", "rules": "compliance", "sanctions": "compliance"}


def _query(args: argparse.Namespace, sql: str, parameters: tuple[Any, ...] = ()) -> list[Any]:
    # The client's connect() is unannotated.
    connection = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host=args.trino_host, port=args.trino_port, user=args.trino_user or READERS[args.target], catalog="lakehouse"
    )
    with connection:
        cursor = connection.cursor()
        cursor.execute(sql, parameters)
        return list(cursor.fetchall())


def _evaluate_parties(args: argparse.Namespace) -> None:
    truth = parties.read_truth(args.ground_truth_dir / "customer_parties.csv")
    resolution = parties.read_resolution(_query(args, "SELECT customer_id, party_id FROM silver.party_member"))
    result = parties.score(resolution, truth)
    log.info(
        "party resolution: precision %.4f, recall %.4f, F1 %.4f (%s pairs found and confirmed, %s wrongly merged,"
        " %s missed)",
        result.precision,
        result.recall,
        result.f1,
        result.true_positives,
        result.false_positives,
        result.false_negatives,
    )


def _evaluate_rules(args: argparse.Namespace) -> None:
    laundering = rules.read_truth(args.ground_truth_dir / "laundering_transfers.csv")
    period = (args.since or date.min, args.until or date.max)
    [(transfers,)] = _query(
        args, "SELECT count(*) FROM silver.payment_transfer WHERE business_date BETWEEN ? AND ?", period
    )
    if args.since or args.until:
        laundering = _transfers_booked(args, laundering, period)
    evidence = _query(
        args,
        "SELECT rule_id, alert_id, transfer_id FROM gold.aml_alert_transfer WHERE business_date BETWEEN ? AND ?",
        period,
    )
    log.info(
        "%-8s %8s %10s %12s %10s %8s %10s %7s",
        "rule",
        "alerts",
        "alert prec",
        "transfers",
        "laundering",
        "recall",
        "precision",
        "lift",
    )
    for result in rules.score(evidence, laundering, transfers):
        log.info(
            "%-8s %8d %10.4f %12d %10d %8.4f %10.4f %7.1f",
            result.rule_id,
            result.alerts,
            result.alert_precision,
            result.flagged_transfers,
            result.true_positives,
            result.recall,
            result.precision,
            result.lift,
        )
    log.info("%d labelled transfers out of %d", len(laundering), transfers)


def _transfers_booked(args: argparse.Namespace, transfer_ids: set[str], period: tuple[date, date]) -> set[str]:
    """The labelled transfers booked in the scored period; the label file has no dates."""
    ordered = sorted(transfer_ids)
    sql = (
        "SELECT transfer_id FROM silver.payment_transfer"
        " WHERE business_date BETWEEN ? AND ? AND contains(?, transfer_id)"
    )
    return {
        transfer_id
        for start in range(0, len(ordered), 1000)
        for (transfer_id,) in _query(args, sql, (*period, ordered[start : start + 1000]))
    }


def _evaluate_sanctions(args: argparse.Namespace) -> None:
    truth = sanctions.read_truth(args.ground_truth_dir / "sanctioned_customers.csv")
    hits = _query(args, "SELECT customer_id, list_name, entry_id FROM gold.sanctions_hit")
    result = sanctions.score(hits, truth)
    log.info(
        "sanctions screening: precision %.4f, recall %.4f (%s planted customers hit, %s on the entry they were"
        " planted from; %s other customers hit; %s planted customers missed)",
        result.precision,
        result.recall,
        result.true_positives,
        result.right_entry,
        result.false_positives,
        result.false_negatives,
    )


EVALUATIONS = {"parties": _evaluate_parties, "rules": _evaluate_rules, "sanctions": _evaluate_sanctions}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="aml-evaluate", description=__doc__)
    parser.add_argument("target", choices=sorted(EVALUATIONS))
    parser.add_argument("--ground-truth-dir", type=Path, default=Path("data/ground_truth"))
    parser.add_argument("--trino-host", default=os.environ.get("TRINO_HOST", "localhost"))
    parser.add_argument("--trino-port", type=int, default=int(os.environ.get("TRINO_PORT", "8080")))
    parser.add_argument("--trino-user", help="defaults to the role that reads the evaluated output")
    parser.add_argument(
        "--from",
        dest="since",
        type=date.fromisoformat,
        help="rules: score business dates from this one (YYYY-MM-DD) only",
    )
    parser.add_argument(
        "--until", type=date.fromisoformat, help="rules: score business dates up to this one (YYYY-MM-DD) only"
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    EVALUATIONS[args.target](args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
