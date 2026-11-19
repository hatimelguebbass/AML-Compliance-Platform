"""Read access to the data contracts in ``contracts/`` (Open Data Contract Standard v3.1).

Contracts are the single definition of each source's structure: producers are
tested against them and ingestion jobs derive their schemas from them, so a
change of structure is a reviewed change to a contract first.

This module is also imported by the Spark jobs, whose interpreter is Python 3.10.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

CONTRACTS_DIR = Path(__file__).resolve().parents[2] / "contracts"


@dataclass(frozen=True)
class Property:
    name: str
    logical_type: str
    physical_type: str = ""
    required: bool = False
    primary_key: bool = False
    format: str | None = None
    pattern: str | None = None
    valid_values: tuple[str, ...] | None = None
    items: Property | None = None
    properties: tuple[Property, ...] = ()


@dataclass(frozen=True)
class UniquenessRule:
    properties: tuple[str, ...]
    severity: str


@dataclass(frozen=True)
class SchemaObject:
    name: str
    physical_name: str
    properties: tuple[Property, ...]
    uniqueness: tuple[UniquenessRule, ...] = field(default=())

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(p.name for p in self.properties)

    @property
    def primary_key(self) -> tuple[str, ...]:
        return tuple(p.name for p in self.properties if p.primary_key)


@dataclass(frozen=True)
class Contract:
    id: str
    version: str
    objects: tuple[SchemaObject, ...]

    def object(self, name: str) -> SchemaObject:
        for candidate in self.objects:
            if candidate.name == name:
                return candidate
        raise KeyError(f"contract {self.id} has no schema object {name!r}")


def _valid_values(raw: dict[str, Any]) -> tuple[str, ...] | None:
    for rule in raw.get("quality", ()):
        if rule.get("metric") == "invalidValues":
            return tuple(str(v) for v in rule["arguments"]["validValues"])
    return None


def _property(raw: dict[str, Any]) -> Property:
    items = raw.get("items")
    return Property(
        name=raw["name"],
        logical_type=raw["logicalType"],
        physical_type=raw.get("physicalType", ""),
        required=bool(raw.get("required", False)),
        primary_key=bool(raw.get("primaryKey", False)),
        format=raw.get("logicalTypeOptions", {}).get("format"),
        pattern=raw.get("logicalTypeOptions", {}).get("pattern"),
        valid_values=_valid_values(raw),
        items=_property({"name": "element", **items}) if items else None,
        properties=tuple(_property(p) for p in raw.get("properties", ())),
    )


def _uniqueness(raw: dict[str, Any], properties: tuple[Property, ...]) -> tuple[UniquenessRule, ...]:
    rules = tuple(
        UniquenessRule(tuple(rule["arguments"]["properties"]), rule.get("severity", "error"))
        for rule in raw.get("quality", ())
        if rule.get("metric") == "duplicateValues"
    )
    primary_key = tuple(p.name for p in properties if p.primary_key)
    if primary_key and all(rule.properties != primary_key for rule in rules):
        # A declared primary key is a uniqueness promise even without an explicit quality rule.
        rules += (UniquenessRule(primary_key, "error"),)
    return rules


def _schema_object(raw: dict[str, Any]) -> SchemaObject:
    properties = tuple(_property(p) for p in raw.get("properties", ()))
    return SchemaObject(
        name=raw["name"],
        physical_name=raw.get("physicalName", raw["name"]),
        properties=properties,
        uniqueness=_uniqueness(raw, properties),
    )


def load(path: Path) -> Contract:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    objects = tuple(_schema_object(obj) for obj in document.get("schema", ()))
    return Contract(id=document["id"], version=document["version"], objects=objects)


def load_by_id(contract_id: str, directory: Path = CONTRACTS_DIR) -> Contract:
    for path in sorted(directory.glob("*.odcs.yaml")):
        contract = load(path)
        if contract.id == contract_id:
            return contract
    raise KeyError(f"no contract with id {contract_id!r} in {directory}")
