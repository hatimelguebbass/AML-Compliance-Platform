"""Precision and recall of the AML rules against the laundering label.

The label marks transfers, so a rule is scored on the transfers its alerts hold as
evidence: a flagged transfer the label confirms is a true positive. Investigators work
alerts, not transfers, so the share of alerts holding at least one labelled transfer is
reported too. Laundering is rare, so precision is read against the base rate: lift is how
many times more often a flagged transfer is laundering than a transfer picked at random.
"""

from __future__ import annotations

import csv
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

ALL_RULES = "ALL"


@dataclass(frozen=True)
class RuleScore:
    rule_id: str
    alerts: int
    alerts_with_laundering: int
    flagged_transfers: int
    true_positives: int
    laundering_transfers: int
    transfers: int

    @property
    def alert_precision(self) -> float:
        return self.alerts_with_laundering / self.alerts if self.alerts else 0.0

    @property
    def precision(self) -> float:
        return self.true_positives / self.flagged_transfers if self.flagged_transfers else 0.0

    @property
    def recall(self) -> float:
        return self.true_positives / self.laundering_transfers if self.laundering_transfers else 1.0

    @property
    def lift(self) -> float:
        base_rate = self.laundering_transfers / self.transfers if self.transfers else 0.0
        return self.precision / base_rate if base_rate else 0.0


def score(evidence: Iterable[tuple[str, str, str]], laundering: set[str], transfers: int) -> list[RuleScore]:
    """Score each rule, then every rule together, from ``(rule_id, alert_id, transfer_id)`` rows."""
    alerts: defaultdict[str, defaultdict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for rule_id, alert_id, transfer_id in evidence:
        alerts[rule_id][alert_id].add(transfer_id)
        alerts[ALL_RULES][f"{rule_id}|{alert_id}"].add(transfer_id)

    scores = []
    for rule_id in sorted(alerts, key=lambda rule: (rule == ALL_RULES, rule)):
        flagged = set().union(*alerts[rule_id].values())
        scores.append(
            RuleScore(
                rule_id=rule_id,
                alerts=len(alerts[rule_id]),
                alerts_with_laundering=sum(1 for held in alerts[rule_id].values() if held & laundering),
                flagged_transfers=len(flagged),
                true_positives=len(flagged & laundering),
                laundering_transfers=len(laundering),
                transfers=transfers,
            )
        )
    return scores


def read_truth(path: Path) -> set[str]:
    with path.open(encoding="utf-8", newline="") as handle:
        return {row["transfer_id"] for row in csv.DictReader(handle)}
