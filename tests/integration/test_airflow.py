"""The Airflow services parse the DAGs and can start Spark jobs, and nothing more, on the Docker host."""

from __future__ import annotations

import json
import subprocess

import pytest

pytestmark = pytest.mark.integration

SCHEDULER = "aml-airflow-scheduler"


def _in_scheduler(*command: str) -> str:
    result = subprocess.run(
        ["docker", "exec", SCHEDULER, *command], capture_output=True, text=True, timeout=120, check=True
    )
    return result.stdout


def _docker_api_status(path: str) -> int:
    """HTTP status of a Docker API call made through the proxy, from inside the scheduler."""
    script = (
        "import urllib.request, urllib.error\n"
        f"try: print(urllib.request.urlopen('http://docker-socket-proxy:2375{path}').status)\n"
        "except urllib.error.HTTPError as e: print(e.code)"
    )
    return int(_in_scheduler("python", "-c", script).strip())


def test_dags_import_without_errors() -> None:
    assert json.loads(_in_scheduler("airflow", "dags", "list-import-errors", "--output", "json")) == []
    dags = json.loads(_in_scheduler("airflow", "dags", "list", "--output", "json"))
    assert "aml_daily" in {d["dag_id"] for d in dags}


def test_spark_pool_has_a_single_slot() -> None:
    pools = json.loads(_in_scheduler("airflow", "pools", "get", "spark", "--output", "json"))
    assert [p["slots"] for p in pools] == ["1"]


def test_proxy_exposes_containers_and_images_only() -> None:
    assert _docker_api_status("/version") == 200
    assert _docker_api_status("/images/json") == 200
    assert _docker_api_status("/containers/json") == 200
    assert _docker_api_status("/volumes") == 403
    assert _docker_api_status("/networks") == 403
    assert _docker_api_status("/exec/unknown/json") == 403
