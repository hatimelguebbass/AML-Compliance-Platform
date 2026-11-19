"""Guards the rule of ADR 0004: labels in data/ground_truth are never reachable by a processing engine."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

COMPOSE = Path(__file__).resolve().parents[2] / "docker-compose.yml"


def _volumes() -> list[tuple[str, str]]:
    services: dict[str, Any] = yaml.safe_load(COMPOSE.read_text())["services"]
    return [(name, volume) for name, service in services.items() for volume in service.get("volumes", [])]


@pytest.mark.parametrize(("service", "volume"), _volumes())
def test_no_service_can_see_ground_truth(service: str, volume: str) -> None:
    source = volume.split(":", 1)[0]
    assert "ground_truth" not in source
    assert source.rstrip("/") not in {"./data", "data", "."}, f"{service} mounts a parent of data/ground_truth"


def test_spark_reads_the_landing_zone_read_only() -> None:
    spark_volumes = [volume for service, volume in _volumes() if service == "spark"]
    assert sum(volume.endswith(":/data/landing:ro") for volume in spark_volumes) == 1
