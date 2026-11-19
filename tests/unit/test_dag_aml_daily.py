"""The daily DAG loads, and every Spark command it renders is accepted by the ingestion job."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from airflow.dag_processing.dagbag import DagBag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

from aml_platform.ingestion import cli
from aml_platform.orchestration.staging import OPEN_TASK, PUBLISH_TASK

DAGS = Path(__file__).resolve().parents[2] / "dags"
TRANSFORM = DAGS.parent / "transform"
HOST_DIR = "/srv/aml"


@pytest.fixture(scope="module")
def dag() -> DAG:
    with pytest.MonkeyPatch.context() as env:
        env.setenv("AML_HOST_DIR", HOST_DIR)
        # The Makefile exports these for compose; here they must default to the host directory.
        env.delenv("AML_LANDING_DIR", raising=False)
        env.delenv("AML_QUARANTINE_DIR", raising=False)
        bag = DagBag(dag_folder=DAGS, known_pools={"default_pool", "spark"})
    assert bag.import_errors == {}
    return bag.dags["aml_daily"]


def _spark_tasks(dag: DAG) -> list[DockerOperator]:
    return [t for t in dag.tasks if isinstance(t, DockerOperator) and t.image == "aml/spark:local"]


def _dbt_tasks(dag: DAG) -> list[DockerOperator]:
    return [t for t in dag.tasks if isinstance(t, DockerOperator) and t.image == "aml/dbt:local"]


def test_a_missing_host_directory_is_an_import_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AML_HOST_DIR", raising=False)
    bag = DagBag(dag_folder=DAGS, known_pools={"default_pool", "spark"})
    assert any("AML_HOST_DIR" in error for error in bag.import_errors.values())


def test_runs_never_overlap_and_publish_comes_last(dag: DAG) -> None:
    assert dag.max_active_runs == 1
    assert dag.get_task(PUBLISH_TASK).upstream_task_ids == {"audit_observability"}
    assert dag.get_task("audit_observability").upstream_task_ids == {"build_observability"}
    assert dag.get_task("build_observability").upstream_task_ids == {"audit_gold"}
    assert dag.get_task("audit_gold").upstream_task_ids == {"build_gold"}
    assert dag.get_task("build_gold").upstream_task_ids == {"audit_silver"}
    assert dag.get_task("audit_silver").upstream_task_ids == {"build_silver"}
    assert dag.get_task("build_silver").upstream_task_ids == {"audit_bronze"}
    assert dag.get_task(OPEN_TASK).upstream_task_ids == {"wait_for_deliveries"}
    ingestions = {t.task_id for t in _spark_tasks(dag)} - {"audit_bronze"}
    assert dag.get_task(OPEN_TASK).downstream_task_ids == ingestions
    assert dag.get_task("audit_bronze").upstream_task_ids == ingestions


def test_spark_jobs_share_one_pool_and_audits_are_not_retried(dag: DAG) -> None:
    jobs = _spark_tasks(dag)
    assert {t.pool for t in jobs} == {"spark"}
    assert dag.get_task("audit_bronze").retries == 0
    assert all(t.retries > 0 for t in jobs if t.task_id != "audit_bronze")


def test_spark_jobs_never_mount_ground_truth(dag: DAG) -> None:
    for task in _spark_tasks(dag):
        mounts = {m["Target"]: m for m in task.mounts}
        assert all("ground_truth" not in m["Source"] for m in mounts.values())
        assert mounts["/data/landing"]["ReadOnly"] is True
        assert all(m["Source"].startswith(HOST_DIR) for m in mounts.values())


def test_every_spark_command_parses_with_the_ingestion_cli(dag: DAG) -> None:
    for task in _spark_tasks(dag):
        command = task.command
        assert isinstance(command, list)
        assert command[:2] == ["/opt/spark/bin/spark-submit", "jobs/ingest.py"]
        arguments = [a.replace("{{ ti.xcom_pull(task_ids='wait_for_deliveries') }}", "2022-09-01") for a in command[2:]]
        args = cli.parse_args(arguments)
        # The audit finds this run's ingestions by the "<run_id>#" prefix of their run ids.
        assert args.run_id == "{{ run_id }}#{{ ti.try_number }}"
        if args.command == "audit":
            assert args.pipeline_run == "{{ run_id }}"
            ingested = {t.command[2] for t in _spark_tasks(dag) if t.task_id != "audit_bronze"}
            assert set(args.pipeline_feeds) == ingested
            # Without it the audit cannot tell a rejected file of the date from a quiet day.
            assert args.business_date is not None
        elif args.command in ("payments", "cards"):
            assert args.business_date is not None


@pytest.mark.parametrize("layer", ["silver", "gold"])
def test_dbt_builds_then_tests_each_layer_on_its_own(dag: DAG, layer: str) -> None:
    build, audit = dag.get_task(f"build_{layer}"), dag.get_task(f"audit_{layer}")
    assert isinstance(build, DockerOperator)
    assert isinstance(audit, DockerOperator)
    assert build.command[:3] == ["build", "--selector", layer]
    assert "test" not in build.command[: build.command.index("--vars")]
    assert audit.command[:5] == ["test", "--selector", layer, "--exclude-resource-type", "unit_test"]
    assert audit.retries == 0
    assert build.retries > 0


def test_every_model_layer_belongs_to_exactly_one_dbt_selector() -> None:
    selectors = yaml.safe_load((TRANSFORM / "selectors.yml").read_text(encoding="utf-8"))["selectors"]
    selected = [item["value"] for selector in selectors for item in selector["definition"]["union"]]
    layers = [f"models/{path.name}" for path in (TRANSFORM / "models").iterdir() if path.is_dir()]
    assert {layer: selected.count(layer) for layer in layers} == dict.fromkeys(layers, 1)


def test_dbt_jobs_run_the_business_date_on_the_staging_branch(dag: DAG) -> None:
    for task in _dbt_tasks(dag):
        variables = task.command[task.command.index("--vars") + 1]
        assert '"business_date": "{{ ti.xcom_pull(task_ids=\'wait_for_deliveries\') }}"' in variables
        mounts = {m["Target"]: m for m in task.mounts}
        assert set(mounts) == {"/opt/dbt/project"}
        assert mounts["/opt/dbt/project"]["Source"] == f"{HOST_DIR}/transform"
        assert mounts["/opt/dbt/project"]["ReadOnly"] is True


def test_change_events_are_dated_with_the_run_business_date(dag: DAG) -> None:
    command = dag.get_task("ingest_corebanking").command
    assert isinstance(command, list)
    assert "--business-date" in command
