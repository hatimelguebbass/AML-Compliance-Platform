"""Contract checks on the records of one file, computed in a single pass.

Raw values are read as strings and cast with ``try_`` functions, so a bad value
becomes a counted violation instead of an exception that hides how many records
are affected. A file with any violation is rejected as a whole: loading the
good part would leave a gap that nobody downstream can see.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from aml_platform.contracts import Property, SchemaObject
from aml_platform.ingestion.schema import spark_type

MALFORMED = "_malformed"
_RAW = "__raw_"


@dataclass(frozen=True)
class Validated:
    records: DataFrame
    record_count: int
    violations: Mapping[str, int]
    duplicates_dropped: int

    def describe_violations(self) -> str:
        return ", ".join(f"{name}={count}" for name, count in sorted(self.violations.items()))


def typed(raw: Column, prop: Property) -> Column:
    target = spark_type(prop)
    if prop.logical_type == "timestamp" and prop.format:
        return F.try_to_timestamp(raw, F.lit(prop.format)).cast(target)
    return raw.try_cast(target)


def _count_if(condition: Column, name: str) -> Column:
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0)).alias(name)


def _record_checks(obj: SchemaObject, has_malformed: bool) -> list[Column]:
    checks = [_count_if(F.col(MALFORMED), "malformed_records")] if has_malformed else []
    for prop in obj.properties:
        value, raw = F.col(prop.name), F.col(_RAW + prop.name)
        checks.append(_count_if(raw.isNotNull() & value.isNull(), f"{prop.name}:not_{prop.logical_type}"))
        if prop.required:
            checks.append(_count_if(raw.isNull(), f"{prop.name}:missing"))
        if prop.pattern is not None:
            checks.append(_count_if(~value.rlike(prop.pattern), f"{prop.name}:pattern_mismatch"))
        if prop.valid_values is not None:
            checks.append(_count_if(~value.isin(*prop.valid_values), f"{prop.name}:invalid_value"))
    return checks


def _duplicate_keys(records: DataFrame, keys: tuple[str, ...]) -> int:
    return records.groupBy(*keys).count().where(F.col("count") > 1).count()


def validate(raw: DataFrame, obj: SchemaObject) -> Validated:
    """Type ``raw`` (one string column per contract property) and count every contract violation.

    ``raw`` may carry a boolean ``_malformed`` column flagging records whose
    structure could not be split into the contract's fields.

    Uniqueness rules of severity ``error`` reject the file on any repeated key. With
    a lower severity, records repeated identically (a resend) are dropped, and only
    a key repeated with different content is a violation.
    """
    has_malformed = MALFORMED in raw.columns
    columns = [typed(F.col(p.name), p).alias(p.name) for p in obj.properties]
    raws = [F.col(p.name).alias(_RAW + p.name) for p in obj.properties]
    flags = [F.col(MALFORMED)] if has_malformed else []
    checked = raw.select(*columns, *raws, *flags).cache()

    counts = checked.agg(F.count(F.lit(1)).alias("records"), *_record_checks(obj, has_malformed)).collect()[0].asDict()
    record_count = int(counts.pop("records"))
    violations = {name: int(count) for name, count in counts.items() if count}

    records = checked.select(*obj.column_names)
    duplicates_dropped = 0
    for rule in obj.uniqueness:
        if rule.severity != "error":
            deduplicated = records.dropDuplicates()
            duplicates_dropped = record_count - deduplicated.count()
            records = deduplicated
        repeated = _duplicate_keys(records, rule.properties)
        if repeated:
            violations[f"{'+'.join(rule.properties)}:duplicate_key"] = repeated
    return Validated(records, record_count, violations, duplicates_dropped)
