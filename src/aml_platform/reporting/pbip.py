"""Static checks of the Power BI project (``bi/``) against itself and the dbt model contracts.

Power BI Desktop is the only tool that fully loads a PBIP project, and it runs on Windows. These
checks catch in CI what would otherwise surface as a broken visual or a failed refresh: a report
field the model does not define, a DAX reference to nothing, a source column a dbt model no longer
has, or a type the model would silently coerce.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_OBJECT = re.compile(r"^(table|column|measure|partition)\s+('(?:[^']|'')+'|[^\s=]+)(?:\s*=\s*(.*))?$")
_PROPERTY = re.compile(r"^(\w+):\s*(.*)$")
# The Trino connector names each table after its schema: Lakehouse{[Name = "gold"]}[Data]{[Name = "gold.aml_alert"]}.
_NAVIGATION = re.compile(r'\{\[Name = "([^"]+)"\]\}\[Data\]\{\[Name = "\1\.([^"]+)"\]\}')
_DAX_COLUMN = re.compile(r"(?:'((?:[^']|'')+)'|\b([A-Za-z_]\w*))\s*\[([^\]]+)\]")
# What is left in brackets once the table[column] references are taken out: measures.
_DAX_MEASURE = re.compile(r"\[([^\]]+)\]")

# dbt contract types and the TMDL data type each must load as.
_TMDL_TYPE = {
    "varchar": "string",
    "bigint": "int64",
    "integer": "int64",
    "boolean": "boolean",
    "double": "double",
    "date": "dateTime",
}


def _unquote(name: str) -> str:
    return name[1:-1].replace("''", "'") if name.startswith("'") else name


@dataclass
class Column:
    name: str
    data_type: str
    source_column: str | None


@dataclass
class Table:
    name: str
    columns: dict[str, Column] = field(default_factory=dict)
    measures: dict[str, str] = field(default_factory=dict)
    source: tuple[str, str] | None = None
    """(schema, table) read from the lakehouse, None for a calculated table."""


def _blocks(lines: list[str]) -> Iterator[tuple[str, str, str, list[str]]]:
    """Yields (kind, name, expression, body lines) for each object one tab inside a table."""
    current: tuple[str, str, str] | None = None
    body: list[str] = []
    for line in lines:
        if line.startswith("\t") and not line.startswith("\t\t"):
            match = _OBJECT.match(line.strip())
            if current:
                yield (*current, body)
            current, body = None, []
            if match:
                current = (match[1], _unquote(match[2]), match[3] or "")
        elif current:
            body.append(line)
    if current:
        yield (*current, body)


def _expression(inline: str, body: list[str]) -> str:
    """The object's expression: inline after '=', or the deeper-indented lines that follow it."""
    if inline.strip():
        return inline
    lines = []
    for line in body:
        if not line.startswith("\t\t\t"):
            break
        lines.append(line.strip())
    return "\n".join(lines).strip("`\n")


def load_table(path: Path) -> Table:
    lines = path.read_text(encoding="utf-8").splitlines()
    header = next(line for line in lines if line.startswith("table "))
    table = Table(_unquote(header.removeprefix("table ").strip()))
    for kind, name, inline, body in _blocks(lines):
        properties = {
            match[1]: match[2] for line in body if line.startswith("\t\t") and (match := _PROPERTY.match(line.strip()))
        }
        if kind == "column":
            table.columns[name] = Column(name, properties["dataType"], properties.get("sourceColumn"))
        elif kind == "measure":
            table.measures[name] = _expression(inline, body)
        elif kind == "partition" and (navigation := _NAVIGATION.search("\n".join(body))):
            table.source = (navigation[1], navigation[2])
    return table


def load_model(model_dir: Path) -> dict[str, Table]:
    tables = [load_table(path) for path in sorted((model_dir / "definition" / "tables").glob("*.tmdl"))]
    return {table.name: table for table in tables}


def report_fields(report_dir: Path) -> set[tuple[str, str, str]]:
    """(kind, entity, property) of every model field the report's pages and visuals use."""
    found: set[tuple[str, str, str]] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for kind in ("Column", "Measure"):
                ref = node.get(kind)
                if isinstance(ref, dict) and "Entity" in ref.get("Expression", {}).get("SourceRef", {}):
                    found.add((kind, ref["Expression"]["SourceRef"]["Entity"], ref["Property"]))
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    for path in sorted((report_dir / "definition").rglob("*.json")):
        walk(json.loads(path.read_text(encoding="utf-8")))
    return found


def dbt_columns(models_dir: Path) -> dict[tuple[str, str], dict[str, str]]:
    """Contract columns and types of every dbt model, keyed by (layer, model): the folder is the schema."""
    contracts: dict[tuple[str, str], dict[str, str]] = {}
    for path in sorted(models_dir.rglob("_*.yml")):
        layer = path.relative_to(models_dir).parts[0]
        for model in (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("models", []):
            columns = {c["name"]: c.get("data_type", "") for c in model.get("columns", [])}
            contracts[(layer, model["name"])] = columns
    return contracts


def _expected_type(dbt_type: str) -> str | None:
    base = dbt_type.split("(")[0].strip().lower()
    if base == "decimal":
        return "decimal"
    if base == "timestamp":
        return "dateTime"
    return _TMDL_TYPE.get(base)


def _dax_problems(model: dict[str, Table], table: Table, measure: str, expression: str) -> Iterator[str]:
    for quoted, bare, column in _DAX_COLUMN.findall(expression):
        name = (quoted or bare).replace("''", "'")
        target = model.get(name)
        if target is None:
            yield f"measure {table.name}[{measure}] reads table {name!r}, which the model does not define"
        elif column not in target.columns and column not in target.measures:
            yield f"measure {table.name}[{measure}] reads {name}[{column}], which the model does not define"
    measures = {m for t in model.values() for m in t.measures}
    stripped = _DAX_COLUMN.sub("", expression)
    for name in _DAX_MEASURE.findall(stripped):
        if name not in measures:
            yield f"measure {table.name}[{measure}] reads measure [{name}], which the model does not define"


def problems(project_dir: Path, models_dir: Path) -> list[str]:
    """Every inconsistency of the Power BI project in ``project_dir`` (bi/) with itself and dbt."""
    model = load_model(next(project_dir.glob("*.SemanticModel")))
    contracts = dbt_columns(models_dir)
    found: list[str] = []

    for kind, entity, prop in sorted(report_fields(next(project_dir.glob("*.Report")))):
        table = model.get(entity)
        defined = table and (table.columns if kind == "Column" else table.measures)
        if not defined or prop not in defined:
            found.append(f"the report uses {kind.lower()} {entity}[{prop}], which the model does not define")

    for table in model.values():
        for measure, expression in table.measures.items():
            found.extend(_dax_problems(model, table, measure, expression))
        if table.source is None:
            continue
        contract = contracts.get(table.source)
        if contract is None:
            found.append(f"table {table.name} reads {'.'.join(table.source)}, which no dbt model defines")
            continue
        for column in table.columns.values():
            if column.source_column not in contract:
                found.append(f"{table.name}[{column.name}] reads {column.source_column}, not in the dbt contract")
                continue
            expected = _expected_type(contract[column.source_column])
            if expected != column.data_type:
                found.append(
                    f"{table.name}[{column.name}] is {column.data_type}, its dbt contract type "
                    f"{contract[column.source_column]} loads as {expected}"
                )
    return found


def source_columns(project_dir: Path) -> dict[tuple[str, str], list[str]]:
    """The lakehouse columns each model table reads, by (schema, table)."""
    model = load_model(next(project_dir.glob("*.SemanticModel")))
    return {
        table.source: [c.source_column for c in table.columns.values() if c.source_column]
        for table in model.values()
        if table.source
    }
