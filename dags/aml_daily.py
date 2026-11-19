"""Daily load of one business date into bronze, silver and gold, checked, published through write-audit-publish.

```
wait_for_deliveries -> open_staging -> ingest_payments    -> audit_bronze -> build_silver -> audit_silver
                                    -> ingest_cards
                                    -> ingest_sanctions
                                    -> ingest_corebanking

audit_silver -> build_gold -> audit_gold -> build_observability -> audit_observability -> publish
```

A run owns the business date of its logical date, which for a scheduled run is the
day that just ended: the payments and card files of that date, plus every sanctions
snapshot and change event that arrived since the last publish. Silver is rebuilt from bronze for that
date by dbt, then the AML rules run on it into gold; gold is only built on an audited silver. Last, the
date is reconciled layer by layer and with the core banking, and its volumes and distributions are compared
with the previous dates (ADR 0010). Everything is written on the Nessie branch `etl`, reset to `main` at
the start, and merged into `main` only when every audit passes.

Recovery: clearing a failed task resumes the run on the same staging branch, since
every ingestion is idempotent. Clearing the whole run starts again from `main`.
Replaying past dates: `make airflow-backfill FROM=2022-09-01 TO=2022-09-18`.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import CronDataIntervalTimetable, PokeReturnValue, chain, dag, task
from airflow.sdk.types import DagRunProtocol, RuntimeTaskInstanceProtocol
from docker.types import Mount

from aml_platform.orchestration.staging import OPEN_TASK, PUBLISH_TASK

log = logging.getLogger(__name__)

NESSIE_URL = os.environ.get("NESSIE_URL", "http://nessie:19120")
# The landing zone as mounted in the Airflow workers, read-only, to sense deliveries.
LANDING_DIR = Path("/data/landing")
# Spark jobs run as sibling containers of the Docker host, so their bind mounts are host paths.
HOST_DIR = os.environ.get("AML_HOST_DIR", "")
HOST_LANDING_DIR = os.environ.get("AML_LANDING_DIR") or f"{HOST_DIR}/data/landing"
HOST_QUARANTINE_DIR = os.environ.get("AML_QUARANTINE_DIR") or f"{HOST_DIR}/data/quarantine"
DOCKER_URL = os.environ.get("AML_DOCKER_URL", "tcp://docker-socket-proxy:2375")
DOCKER_NETWORK = os.environ.get("AML_DOCKER_NETWORK", "aml_default")
SPARK_IMAGE = os.environ.get("AML_SPARK_IMAGE", "aml/spark:local")
SPARK_USER = os.environ.get("AML_SPARK_USER", "185:185")
DBT_IMAGE = os.environ.get("AML_DBT_IMAGE", "aml/dbt:local")
# Layers are selected as transform/selectors.yml defines them.
SILVER = ("--selector", "silver")
GOLD = ("--selector", "gold")
OBSERVABILITY = ("--selector", "observability")
# One Spark job at a time: each driver takes 2 GB of a 16 GB workstation.
SPARK_POOL = "spark"

BUSINESS_DATE = "{{ ti.xcom_pull(task_ids='wait_for_deliveries') }}"
# Recorded in the audit tables: links every bronze row to the Airflow run and attempt that wrote it.
RUN_ID = "{{ run_id }}#{{ ti.try_number }}"

if not HOST_DIR.startswith("/"):
    raise RuntimeError("AML_HOST_DIR must be the repository's absolute path on the Docker host (set by the Makefile)")


def dbt_job(task_id: str, *arguments: str, retries: int = 2) -> DockerOperator:
    """A dbt command in the dbt image, on the staging branch, for the run's business date."""
    variables = f'{{"business_date": "{BUSINESS_DATE}", "pipeline_run": "{RUN_ID}"}}'
    return DockerOperator(
        task_id=task_id,
        image=DBT_IMAGE,
        command=[*arguments, "--vars", variables],
        docker_url=DOCKER_URL,
        network_mode=DOCKER_NETWORK,
        mem_limit="1g",
        environment={"TRINO_HOST": "trino"},
        mounts=[Mount("/opt/dbt/project", f"{HOST_DIR}/transform", type="bind", read_only=True)],
        mount_tmp_dir=False,
        auto_remove="force",
        do_xcom_push=False,
        retries=retries,
        execution_timeout=timedelta(hours=1),
    )


def spark_job(task_id: str, *arguments: str, retries: int = 2) -> DockerOperator:
    """A ``jobs/ingest.py`` command in the Spark image, configured like the compose ``spark`` service."""
    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        command=["/opt/spark/bin/spark-submit", "jobs/ingest.py", *arguments, "--run-id", RUN_ID],
        docker_url=DOCKER_URL,
        network_mode=DOCKER_NETWORK,
        user=SPARK_USER,
        mem_limit="2g",
        environment={
            "AWS_REGION": os.environ.get("S3_REGION", ""),
            "HADOOP_USER_NAME": "spark",
            "SPARK_USER": "spark",
            "PYTHONPATH": "/opt/aml/src",
        },
        # Kept out of the rendered templates shown in the UI.
        private_environment={
            "AWS_ACCESS_KEY_ID": os.environ.get("S3_ACCESS_KEY", ""),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("S3_SECRET_KEY", ""),
        },
        # Same mounts as the compose service: never data/ground_truth (ADR 0004).
        mounts=[
            Mount("/opt/spark/work-dir/jobs", f"{HOST_DIR}/jobs", type="bind", read_only=True),
            Mount("/opt/aml/src", f"{HOST_DIR}/src", type="bind", read_only=True),
            Mount("/opt/aml/contracts", f"{HOST_DIR}/contracts", type="bind", read_only=True),
            Mount("/data/landing", HOST_LANDING_DIR, type="bind", read_only=True),
            Mount("/data/quarantine", HOST_QUARANTINE_DIR, type="bind"),
        ],
        # The default bind-mounts a temporary directory of the Airflow container, which is not a host path.
        mount_tmp_dir=False,
        auto_remove="force",
        do_xcom_push=False,
        pool=SPARK_POOL,
        retries=retries,
        execution_timeout=timedelta(hours=1),
    )


@dag(
    dag_id="aml_daily",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=datetime(2022, 9, 1, tzinfo=UTC),
    catchup=False,
    # Runs share the staging branch. Backfill runs are counted apart from this limit: open_staging covers them.
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
    },
    tags=["aml", "bronze", "silver", "gold"],
    doc_md=__doc__,
)
def aml_daily() -> None:
    @task.sensor(
        poke_interval=timedelta(minutes=10).total_seconds(),
        timeout=timedelta(hours=12).total_seconds(),
        mode="reschedule",
    )
    def wait_for_deliveries(logical_date: datetime | None = None) -> PokeReturnValue:
        """Hold the run until the business date's payments manifest and card file have landed."""
        from aml_platform.orchestration.deliveries import missing_deliveries

        if logical_date is None:
            raise ValueError("the run has no logical date: trigger it with one, it sets the business date")
        business_date = logical_date.date()
        missing = missing_deliveries(LANDING_DIR, business_date)
        if missing:
            log.info("business date %s still waits for: %s", business_date, "; ".join(missing))
        return PokeReturnValue(is_done=not missing, xcom_value=business_date.isoformat())

    @task.sensor(task_id=OPEN_TASK, poke_interval=60, timeout=timedelta(hours=6).total_seconds(), mode="reschedule")
    def open_staging(dag_run: DagRunProtocol | None = None, ti: RuntimeTaskInstanceProtocol | None = None) -> bool:
        """Once no other active run holds ``etl``, point it at ``main``, discarding what a failed run left."""
        from aml_platform.lakehouse.bootstrap import bootstrap
        from aml_platform.orchestration.staging import runs_holding_staging

        if dag_run is None or ti is None:
            raise RuntimeError("open_staging needs its run context")
        dag_id = dag_run.dag_id
        task_states = ti.get_task_states(dag_id=dag_id, task_ids=[OPEN_TASK, PUBLISH_TASK])
        holders = runs_holding_staging(
            task_states, lambda run_id: ti.get_dagrun_state(dag_id=dag_id, run_id=run_id), dag_run.run_id
        )
        if holders:
            log.info("staging branch held by %s; waiting", ", ".join(holders))
            return False
        bootstrap(NESSIE_URL)
        return True

    @task(task_id=PUBLISH_TASK)
    def publish(run_id: str | None = None) -> str:
        """Merge ``etl`` into ``main``: readers see the whole business date at once, or none of it."""
        from aml_platform.lakehouse.publish import publish as merge

        return merge(NESSIE_URL, f"aml_daily {run_id}")

    ingestions = [
        spark_job("ingest_payments", "payments", "--business-date", BUSINESS_DATE),
        spark_job("ingest_cards", "cards", "--business-date", BUSINESS_DATE),
        spark_job("ingest_sanctions", "sanctions"),
        # Changes captured by this run take effect on its business date (ADR 0008).
        spark_job("ingest_corebanking", "corebanking", "--business-date", BUSINESS_DATE),
    ]
    # A failed audit is a data problem: retrying cannot fix it, it needs someone to look.
    audit_bronze = spark_job(
        "audit_bronze", "audit", "--pipeline-run", "{{ run_id }}", "--business-date", BUSINESS_DATE, retries=0
    )
    build_silver = dbt_job("build_silver", "build", *SILVER, "--resource-type", "seed", "--resource-type", "model")
    audit_silver = dbt_job("audit_silver", "test", *SILVER, "--exclude-resource-type", "unit_test", retries=0)
    build_gold = dbt_job("build_gold", "build", *GOLD, "--resource-type", "model")
    audit_gold = dbt_job("audit_gold", "test", *GOLD, "--exclude-resource-type", "unit_test", retries=0)
    build_observability = dbt_job("build_observability", "build", *OBSERVABILITY, "--resource-type", "model")
    audit_observability = dbt_job(
        "audit_observability", "test", *OBSERVABILITY, "--exclude-resource-type", "unit_test", retries=0
    )
    chain(
        wait_for_deliveries(),
        open_staging(),
        ingestions,
        audit_bronze,
        build_silver,
        audit_silver,
        build_gold,
        audit_gold,
        build_observability,
        audit_observability,
        publish(),
    )


aml_daily()
