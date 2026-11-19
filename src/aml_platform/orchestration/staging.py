"""One pipeline run at a time on the staging branch.

Airflow's ``max_active_runs`` counts backfill runs apart from scheduled and manual
ones, so two runs of the daily pipeline can be active together. They share the
branch ``etl``: a run that resets it discards the other's unpublished work, and
that run would then publish its business date with feeds missing. A run therefore
opens the branch only when no other active run holds it.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

OPEN_TASK = "open_staging"
PUBLISH_TASK = "publish"


def runs_holding_staging(
    task_states: Mapping[str, Mapping[str, str | None]],
    run_state: Callable[[str], str],
    own_run_id: str,
) -> list[str]:
    """Other active runs that opened the branch, or are opening it right now, and have not published it.

    ``task_states`` maps run ids to the state of each task; ``run_state`` returns the
    state of a run and is only asked about candidates, since a long history holds
    hundreds of finished runs. A run that failed holds nothing: the next one discards its work.
    """
    candidates = (
        run_id
        for run_id, states in task_states.items()
        if run_id != own_run_id
        and states.get(OPEN_TASK) in ("running", "success")
        and states.get(PUBLISH_TASK) != "success"
    )
    return sorted(run_id for run_id in candidates if run_state(run_id) == "running")
