from __future__ import annotations

from pathlib import Path

import pytest

from aml_platform.evaluation import rules


def test_each_rule_is_scored_on_its_evidence_then_all_rules_together() -> None:
    evidence = [
        ("AML-001", "a1", "t1"),
        ("AML-001", "a1", "t2"),
        ("AML-001", "a2", "t3"),
        ("AML-002", "b1", "t2"),
        ("AML-002", "b1", "t4"),
    ]

    fan_in, fan_out, every_rule = rules.score(evidence, laundering={"t2", "t9"}, transfers=1000)

    assert (fan_in.rule_id, fan_in.alerts, fan_in.alerts_with_laundering) == ("AML-001", 2, 1)
    assert (fan_in.flagged_transfers, fan_in.true_positives) == (3, 1)
    assert fan_in.alert_precision == pytest.approx(0.5)
    assert fan_in.precision == pytest.approx(1 / 3)
    assert fan_in.recall == pytest.approx(0.5)
    assert fan_in.lift == pytest.approx((1 / 3) / (2 / 1000))
    assert fan_out.rule_id == "AML-002"
    # A transfer flagged by two rules counts once for all rules, an alert once per rule.
    assert (every_rule.rule_id, every_rule.alerts, every_rule.flagged_transfers) == (rules.ALL_RULES, 3, 4)
    assert every_rule.true_positives == 1


def test_no_alert_scores_zero_precision() -> None:
    assert rules.score([], laundering={"t1"}, transfers=10) == []
    empty = rules.RuleScore("AML-001", 0, 0, 0, 0, 1, 10)
    assert (empty.alert_precision, empty.precision, empty.recall, empty.lift) == (0.0, 0.0, 0.0, 0.0)


def test_ground_truth_file(tmp_path: Path) -> None:
    path = tmp_path / "laundering_transfers.csv"
    path.write_text("transfer_id,attempt_id,typology\nTRF1,,\nTRF2,7,FAN-OUT\n")
    assert rules.read_truth(path) == {"TRF1", "TRF2"}
