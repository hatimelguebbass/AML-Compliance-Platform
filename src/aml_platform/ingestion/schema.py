"""Translation of contract properties into Spark SQL types.

Bronze tables are typed from the contracts rather than inferred from the files,
so a producer that changes a type is caught by validation instead of silently
widening a column.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from aml_platform.contracts import Property

_DECIMAL = re.compile(r"decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)", re.IGNORECASE)
_SCALARS = {
    "string": "STRING",
    "integer": "BIGINT",
    "boolean": "BOOLEAN",
    "date": "DATE",
}


class ContractTypeError(ValueError):
    """A contract property cannot be mapped to a Spark type without guessing."""


def spark_type(prop: Property) -> str:
    logical = prop.logical_type
    if logical in _SCALARS:
        return _SCALARS[logical]
    if logical == "number":
        # Amounts must stay exact: a number without declared precision is a contract defect, not a double.
        match = _DECIMAL.search(prop.physical_type)
        if match is None:
            raise ContractTypeError(f"{prop.name}: number without decimal(precision,scale) physical type")
        return f"DECIMAL({match.group(1)},{match.group(2)})"
    if logical == "timestamp":
        # A timestamp without zone is a wall-clock reading; storing it as an instant would shift it by the
        # session zone. Only a source column that records an instant becomes TIMESTAMP.
        return "TIMESTAMP" if is_instant(prop) else "TIMESTAMP_NTZ"
    if logical == "array":
        element = spark_type(prop.items) if prop.items is not None else "STRING"
        return f"ARRAY<{element}>"
    if logical == "object":
        if not prop.properties:
            raise ContractTypeError(f"{prop.name}: object without properties")
        return f"STRUCT<{', '.join(f'{quote(p.name)}: {spark_type(p)}' for p in prop.properties)}>"
    raise ContractTypeError(f"{prop.name}: unsupported logical type {logical!r}")


def is_instant(prop: Property) -> bool:
    physical = prop.physical_type.lower()
    return physical == "timestamptz" or "with time zone" in physical


def quote(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def struct_ddl(properties: Iterable[Property]) -> str:
    """Column list in Spark DDL, e.g. ``"`id` STRING, `amount` DECIMAL(22,6)"``."""
    return ", ".join(f"{quote(p.name)} {spark_type(p)}" for p in properties)
