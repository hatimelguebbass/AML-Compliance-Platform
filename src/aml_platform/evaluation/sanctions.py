"""Precision and recall of the sanctions screening against the customers planted on the lists.

Scored per customer: a hit on a planted customer is a true positive, a hit on anyone else
a false positive for compliance to discard, and a planted customer without a hit a miss.
A planted customer is also checked to be hit on the entry it was planted from.
"""

from __future__ import annotations

import csv
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

Entry = tuple[str, str]


@dataclass(frozen=True)
class ScreeningScore:
    true_positives: int
    false_positives: int
    false_negatives: int
    right_entry: int

    @property
    def precision(self) -> float:
        found = self.true_positives + self.false_positives
        return self.true_positives / found if found else 1.0

    @property
    def recall(self) -> float:
        expected = self.true_positives + self.false_negatives
        return self.true_positives / expected if expected else 1.0


def score(hits: Iterable[tuple[int, str, str]], truth: Mapping[int, Entry]) -> ScreeningScore:
    """Compare ``(customer_id, list_name, entry_id)`` hits with each planted customer's entry."""
    entries: defaultdict[int, set[Entry]] = defaultdict(set)
    for customer_id, list_name, entry_id in hits:
        entries[customer_id].add((list_name.upper(), entry_id))
    hit, planted = entries.keys(), truth.keys()
    return ScreeningScore(
        true_positives=len(hit & planted),
        false_positives=len(hit - planted),
        false_negatives=len(planted - hit),
        right_entry=sum(1 for customer, entry in truth.items() if entry in entries.get(customer, set())),
    )


def read_truth(path: Path) -> dict[int, Entry]:
    with path.open(encoding="utf-8", newline="") as handle:
        return {int(row["customer_id"]): (row["list"].upper(), row["list_entry_id"]) for row in csv.DictReader(handle)}
