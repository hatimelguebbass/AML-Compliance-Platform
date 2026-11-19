"""Pairwise precision and recall of the party resolution.

Two customer records form a pair when they belong to the same party. A pair the
resolution finds and the ground truth confirms is a true positive; a found pair the
ground truth denies is a false positive (two people merged); a ground-truth pair the
resolution misses is a false negative (one person left split).
"""

from __future__ import annotations

import csv
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path

Pair = tuple[int, int]


@dataclass(frozen=True)
class PairwiseScore:
    true_positives: int
    false_positives: int
    false_negatives: int

    @property
    def precision(self) -> float:
        found = self.true_positives + self.false_positives
        return self.true_positives / found if found else 1.0

    @property
    def recall(self) -> float:
        expected = self.true_positives + self.false_negatives
        return self.true_positives / expected if expected else 1.0

    @property
    def f1(self) -> float:
        total = self.precision + self.recall
        return 2 * self.precision * self.recall / total if total else 0.0


def pairs(membership: Mapping[int, object]) -> set[Pair]:
    """Every pair of records sharing a group, lower id first."""
    groups: defaultdict[object, list[int]] = defaultdict(list)
    for record, group in membership.items():
        groups[group].append(record)
    return {pair for members in groups.values() for pair in combinations(sorted(members), 2)}


def score(predicted: Mapping[int, object], truth: Mapping[int, object]) -> PairwiseScore:
    """Compare on the records both sides know: customers created after the ground truth are left out."""
    known = predicted.keys() & truth.keys()
    found = pairs({record: predicted[record] for record in known})
    expected = pairs({record: truth[record] for record in known})
    return PairwiseScore(
        true_positives=len(found & expected),
        false_positives=len(found - expected),
        false_negatives=len(expected - found),
    )


def read_truth(path: Path) -> dict[int, object]:
    with path.open(encoding="utf-8", newline="") as handle:
        return {int(row["customer_id"]): row["party_id"] for row in csv.DictReader(handle)}


def read_resolution(rows: Iterable[tuple[int, int]]) -> dict[int, object]:
    return {int(customer_id): int(party_id) for customer_id, party_id in rows}
