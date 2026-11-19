from __future__ import annotations

import pytest

from aml_platform import contracts
from aml_platform.contracts import Property
from aml_platform.ingestion.schema import ContractTypeError, spark_type, struct_ddl

INGESTED_CONTRACTS = (
    "payments-hub-transfers",
    "card-processor-transactions",
    "sanctions-lists",
    "corebanking-customer-master",
)


@pytest.mark.parametrize(
    ("prop", "expected"),
    [
        (Property("id", "string"), "STRING"),
        (Property("uid", "integer", "integer"), "BIGINT"),
        (Property("amount", "number", "decimal(22,6)"), "DECIMAL(22,6)"),
        (Property("flag", "boolean"), "BOOLEAN"),
        (Property("day", "date"), "DATE"),
        (Property("at", "timestamp", "timestamp without time zone"), "TIMESTAMP_NTZ"),
        (Property("at", "timestamp", "timestamp with time zone"), "TIMESTAMP"),
        (Property("at", "timestamp", "timestamptz"), "TIMESTAMP"),
        (Property("tags", "array"), "ARRAY<STRING>"),
        (Property("tags", "array", items=Property("element", "integer")), "ARRAY<BIGINT>"),
        (
            Property("aka", "array", items=Property("element", "object", properties=(Property("name", "string"),))),
            "ARRAY<STRUCT<`name`: STRING>>",
        ),
    ],
)
def test_spark_type(prop: Property, expected: str) -> None:
    assert spark_type(prop) == expected


def test_number_without_declared_precision_is_refused() -> None:
    with pytest.raises(ContractTypeError, match="decimal"):
        spark_type(Property("amount", "number", "double"))


def test_object_without_properties_is_refused() -> None:
    with pytest.raises(ContractTypeError):
        spark_type(Property("blob", "object"))


def test_ddl_quotes_identifiers() -> None:
    assert struct_ddl([Property("type", "string"), Property("we`ird", "integer")]) == "`type` STRING, `we``ird` BIGINT"


@pytest.mark.parametrize("contract_id", INGESTED_CONTRACTS)
def test_every_ingested_object_maps_to_spark(contract_id: str) -> None:
    for obj in contracts.load_by_id(contract_id).objects:
        assert struct_ddl(obj.properties).count("`") >= 2 * len(obj.properties)


def test_payment_amounts_stay_exact() -> None:
    transfer = contracts.load_by_id("payments-hub-transfers").object("payment_transfer")
    types = {p.name: spark_type(p) for p in transfer.properties}
    assert types["amount_paid"] == types["amount_received"] == "DECIMAL(22,6)"
    assert types["booked_at"] == "TIMESTAMP_NTZ"
