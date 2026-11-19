from __future__ import annotations

from pathlib import Path

import pytest

from aml_platform.evaluation import parties


def test_pairs_are_every_combination_within_a_group() -> None:
    assert parties.pairs({3: "a", 1: "a", 2: "a", 4: "b"}) == {(1, 2), (1, 3), (2, 3)}


def test_a_merge_of_two_people_is_a_false_positive_and_a_split_a_false_negative() -> None:
    truth = {1: "p1", 2: "p1", 3: "p2", 4: "p3", 5: "p3"}
    predicted = {1: 1, 2: 1, 3: 1, 4: 4, 5: 5}

    score = parties.score(predicted, truth)

    assert (score.true_positives, score.false_positives, score.false_negatives) == (1, 2, 1)
    assert score.precision == pytest.approx(1 / 3)
    assert score.recall == pytest.approx(1 / 2)
    assert score.f1 == pytest.approx(0.4)


def test_records_unknown_to_the_ground_truth_are_left_out() -> None:
    score = parties.score({1: 1, 2: 1, 99: 1}, {1: "p1", 2: "p1"})
    assert (score.true_positives, score.false_positives, score.false_negatives) == (1, 0, 0)


def test_nothing_to_find_scores_perfectly() -> None:
    score = parties.score({1: 1, 2: 2}, {1: "p1", 2: "p2"})
    assert (score.precision, score.recall) == (1.0, 1.0)


def test_ground_truth_file(tmp_path: Path) -> None:
    path = tmp_path / "customer_parties.csv"
    path.write_text("customer_id,party_id,record_role\n1,P1,original\n7,P1,duplicate\n")
    assert parties.read_truth(path) == {1: "P1", 7: "P1"}
