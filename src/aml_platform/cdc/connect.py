"""Kafka Connect REST client and the Debezium connector configuration."""

from __future__ import annotations

import logging
import time
from typing import Any, Self

import httpx

from aml_platform.cdc.topics import CdcSource

log = logging.getLogger(__name__)

JSON_CONVERTER = "org.apache.kafka.connect.json.JsonConverter"


class ConnectorFailedError(RuntimeError):
    """The connector or one of its tasks is not running."""


def connector_config(source: CdcSource, *, database_host: str = "source-db") -> dict[str, str]:
    """Debezium PostgreSQL connector settings for one captured schema.

    Credentials are references resolved by the Connect worker from its own
    environment: the REST API, its config topic and this repository never hold them.
    """
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "plugin.name": "pgoutput",
        "database.hostname": database_host,
        "database.port": "5432",
        "database.dbname": "${env:SOURCE_DB_NAME}",
        "database.user": "${env:CDC_DB_USER}",
        "database.password": "${env:CDC_DB_PASSWORD}",
        "topic.prefix": source.topic_prefix,
        "schema.include.list": source.schema,
        "table.include.list": ",".join(f"{source.schema}.{table}" for table in source.tables),
        # The publication is created by the database owner with an explicit table list
        # (aml_platform.cdc.source_db): the capture user needs no ownership of the tables.
        "publication.name": source.publication_name,
        "publication.autocreate.mode": "disabled",
        "slot.name": source.slot_name,
        "snapshot.mode": "initial",
        # Compacted topics need tombstones to eventually forget deleted keys.
        "tombstones.on.delete": "true",
        # Keeps the slot's confirmed position moving when captured tables are idle, so the
        # database can recycle WAL written for other tables.
        "heartbeat.interval.ms": "10000",
        "decimal.handling.mode": "string",
        # Events are plain JSON without inline schemas: the data contract is the schema, and bronze
        # ingestion checks every event against it.
        "key.converter": JSON_CONVERTER,
        "key.converter.schemas.enable": "false",
        "value.converter": JSON_CONVERTER,
        "value.converter.schemas.enable": "false",
    }


class KafkaConnect:
    def __init__(self, base_url: str, timeout: float = 30.0, transport: httpx.BaseTransport | None = None) -> None:
        self._http = httpx.Client(base_url=base_url, timeout=timeout, transport=transport)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self._http.close()

    def apply(self, name: str, config: dict[str, str]) -> None:
        """Create the connector or update its configuration in place (PUT is idempotent)."""
        response = self._http.put(f"/connectors/{name}/config", json=config)
        if response.status_code >= 400:
            raise ConnectorFailedError(f"{name}: Kafka Connect refused the configuration: {response.text}")
        log.info("connector %s %s", name, "created" if response.status_code == 201 else "updated")

    def status(self, name: str) -> dict[str, Any]:
        response = self._http.get(f"/connectors/{name}/status")
        response.raise_for_status()
        status: dict[str, Any] = response.json()
        return status

    def wait_until_running(self, name: str, timeout: float = 120.0, poll_interval: float = 1.0) -> None:
        deadline = time.monotonic() + timeout
        while True:
            response = self._http.get(f"/connectors/{name}/status")
            if response.status_code == 200:
                status = response.json()
                states = [status["connector"]["state"], *(task["state"] for task in status["tasks"])]
                if "FAILED" in states:
                    traces = [t.get("trace", "") for t in status["tasks"] if t["state"] == "FAILED"]
                    raise ConnectorFailedError(f"{name} failed: {(traces or [''])[0][:2000]}")
                if status["tasks"] and all(state == "RUNNING" for state in states):
                    log.info("connector %s running", name)
                    return
            if time.monotonic() > deadline:
                raise ConnectorFailedError(f"{name} not running after {timeout:.0f}s: {response.text[:2000]}")
            time.sleep(poll_interval)

    def remove(self, name: str, timeout: float = 60.0) -> None:
        """Delete the connector and its source offsets, so a connector created later under the same name
        starts with a new snapshot instead of resuming from a position whose data may be gone."""
        if self._http.get(f"/connectors/{name}").status_code == 404:
            return
        self._http.put(f"/connectors/{name}/stop").raise_for_status()
        deadline = time.monotonic() + timeout
        while self.status(name)["connector"]["state"] != "STOPPED":
            if time.monotonic() > deadline:
                raise ConnectorFailedError(f"{name} did not stop within {timeout:.0f}s")
            time.sleep(1)
        self._http.delete(f"/connectors/{name}/offsets").raise_for_status()
        self._http.delete(f"/connectors/{name}").raise_for_status()
        log.info("connector %s and its offsets removed", name)
