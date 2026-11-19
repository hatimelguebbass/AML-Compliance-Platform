from __future__ import annotations

import json

import httpx
import pytest

from aml_platform import contracts
from aml_platform.cdc import connect, kafka_admin
from aml_platform.cdc.topics import CHANGE_TOPIC_PARTITIONS, CORE_BANKING, WEEK_MS, CdcSource
from aml_platform.contracts import Property
from aml_platform.ingestion import corebanking

CONTRACT = contracts.load_by_id(corebanking.CONTRACT_ID)


def test_captured_tables_are_the_contract_objects() -> None:
    assert sorted(CORE_BANKING.tables) == sorted(obj.name for obj in CONTRACT.objects)
    assert {obj.physical_name for obj in CONTRACT.objects} == {f"core.{t}" for t in CORE_BANKING.tables}


def test_topics_follow_debezium_naming_and_are_compacted_after_a_week() -> None:
    topics = {topic.name: topic for topic in CORE_BANKING.topics()}
    assert set(topics) == {
        "corebanking.core.customer",
        "corebanking.core.account",
        "corebanking.core.card",
        "__debezium-heartbeat.corebanking",
    }
    customer = topics["corebanking.core.customer"]
    assert customer.partitions == CHANGE_TOPIC_PARTITIONS
    assert customer.config["cleanup.policy"] == "compact"
    assert int(customer.config["min.compaction.lag.ms"]) >= WEEK_MS
    assert topics["__debezium-heartbeat.corebanking"].config["cleanup.policy"] == "delete"


def test_unknown_table_has_no_topic() -> None:
    with pytest.raises(KeyError):
        CORE_BANKING.change_topic("loan")


def test_slot_and_publication_names_are_sql_identifiers() -> None:
    source = CdcSource(topic_prefix="it-1a2b", schema="s", tables=("t",))
    assert source.slot_name == "debezium_it_1a2b"
    assert source.publication_name == source.slot_name


def test_connector_config_holds_no_secret_and_uses_the_managed_publication() -> None:
    config = connect.connector_config(CORE_BANKING)
    assert config["database.password"] == "${env:CDC_DB_PASSWORD}"
    assert config["database.user"] == "${env:CDC_DB_USER}"
    assert config["table.include.list"] == "core.customer,core.account,core.card"
    assert config["publication.autocreate.mode"] == "disabled"
    assert config["publication.name"] == config["slot.name"] == "debezium_corebanking"
    assert config["topic.prefix"] == "corebanking"
    assert config["value.converter.schemas.enable"] == "false"
    assert config["tombstones.on.delete"] == "true"


def _status(connector: str, *tasks: dict[str, str]) -> dict[str, object]:
    return {"name": "c", "connector": {"state": connector}, "tasks": list(tasks)}


def _client(responses: list[dict[str, object]]) -> connect.KafkaConnect:
    def handle(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=responses.pop(0) if len(responses) > 1 else responses[0])

    return connect.KafkaConnect("http://connect:8083", transport=httpx.MockTransport(handle))


def test_wait_until_running_waits_for_every_task() -> None:
    responses = [
        _status("RUNNING"),
        _status("RUNNING", {"id": "0", "state": "UNASSIGNED"}),
        _status("RUNNING", {"id": "0", "state": "RUNNING"}),
    ]
    _client(responses).wait_until_running("c", poll_interval=0)
    assert len(responses) == 1


def test_failed_task_surfaces_its_trace() -> None:
    failed = _status("RUNNING", {"id": "0", "state": "FAILED", "trace": "PSQLException: permission denied"})
    with pytest.raises(connect.ConnectorFailedError, match="permission denied"):
        _client([failed]).wait_until_running("c", poll_interval=0)


def test_end_offsets_are_parsed_per_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        kafka_admin, "_kafka", lambda *_: "corebanking.core.card:0:29110\ncorebanking.core.card:2:30001\n"
    )
    assert kafka_admin.end_offsets("corebanking.core.card") == {0: 29110, 2: 30001}


@pytest.mark.parametrize(
    ("prop", "expected"),
    [
        (Property("birth_date", "date", "date"), "INT"),
        (Property("updated_at", "timestamp", "timestamptz"), "STRING"),
        (Property("booked_at", "timestamp", "timestamp without time zone"), "BIGINT"),
        (Property("amount", "number", "numeric(18,2)"), "STRING"),
        (Property("customer_id", "integer", "bigint"), "BIGINT"),
        (Property("is_pep", "boolean", "boolean"), "BOOLEAN"),
    ],
)
def test_wire_types_follow_the_debezium_json_encoding(prop: Property, expected: str) -> None:
    assert corebanking.wire_type(prop) == expected


def test_every_contract_column_has_a_wire_type() -> None:
    for obj in CONTRACT.objects:
        assert "STRUCT<" in corebanking.envelope_ddl(obj)


def test_starting_offsets_resume_after_the_last_stored_offset() -> None:
    offsets = json.loads(corebanking.starting_offsets("t", {0: 41, 2: 7}))
    assert offsets == {"t": {"0": 42, "1": corebanking.EARLIEST, "2": 8}}
