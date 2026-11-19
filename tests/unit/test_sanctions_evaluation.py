from __future__ import annotations

from pathlib import Path

import pytest

from aml_platform.evaluation import sanctions


def test_hits_are_scored_per_customer_and_checked_against_the_planted_entry() -> None:
    truth = {1: ("OFAC", "100"), 2: ("UN", "QDi.1"), 3: ("OFAC", "300")}
    hits = [(1, "OFAC", "100"), (1, "UN", "QDi.9"), (2, "OFAC", "555"), (8, "OFAC", "100")]

    result = sanctions.score(hits, truth)

    assert (result.true_positives, result.false_positives, result.false_negatives) == (2, 1, 1)
    assert result.right_entry == 1
    assert result.precision == pytest.approx(2 / 3)
    assert result.recall == pytest.approx(2 / 3)


def test_list_names_compare_without_case() -> None:
    assert sanctions.score([(1, "ofac", "100")], {1: ("OFAC", "100")}).right_entry == 1


def test_ground_truth_file(tmp_path: Path) -> None:
    path = tmp_path / "sanctioned_customers.csv"
    path.write_text("customer_id,list,list_entry_id,listed_name\n1153852,ofac,29102,Alireza RAHNAVARD\n")
    assert sanctions.read_truth(path) == {1153852: ("OFAC", "29102")}
