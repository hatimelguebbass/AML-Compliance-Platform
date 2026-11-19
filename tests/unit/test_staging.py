from __future__ import annotations

from aml_platform.orchestration.staging import runs_holding_staging

OWN = "backfill__2022-09-02"


def _never_asked(run_id: str) -> str:
    raise AssertionError(f"state of {run_id} should not be needed")


def test_nothing_holds_the_branch_when_no_other_run_opened_it() -> None:
    states = {OWN: {"open_staging": "running"}, "scheduled__2026-11-12": {"open_staging": None}}
    assert runs_holding_staging(states, _never_asked, OWN) == []


def test_an_active_run_between_open_and_publish_holds_it() -> None:
    other = "scheduled__2026-11-12"
    states = {OWN: {"open_staging": "running"}, other: {"open_staging": "success", "publish": None}}
    assert runs_holding_staging(states, {OWN: "running", other: "running"}.__getitem__, OWN) == [other]


def test_a_run_opening_it_concurrently_holds_it() -> None:
    other = "manual__2026-11-12"
    states = {OWN: {"open_staging": "running"}, other: {"open_staging": "running"}}
    assert runs_holding_staging(states, {OWN: "running", other: "running"}.__getitem__, OWN) == [other]


def test_published_and_failed_runs_release_it() -> None:
    published, failed = "backfill__2022-09-01", "scheduled__2026-11-11"
    states = {
        OWN: {"open_staging": "running"},
        published: {"open_staging": "success", "publish": "success"},
        failed: {"open_staging": "success", "publish": "upstream_failed"},
    }
    assert runs_holding_staging(states, {failed: "failed"}.__getitem__, OWN) == []
