from __future__ import annotations

import json
import shutil
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from aml_platform.reporting import figures, pbip, schemas

ROOT = Path(__file__).parents[2]
PROJECT = ROOT / "bi"
MODELS = ROOT / "transform" / "models"
TABLES = PROJECT / "AmlCompliance.SemanticModel" / "definition" / "tables"


@pytest.fixture
def project(tmp_path: Path) -> Path:
    shutil.copytree(PROJECT, tmp_path / "bi")
    return tmp_path / "bi"


@pytest.fixture
def models(tmp_path: Path) -> Path:
    shutil.copytree(MODELS, tmp_path / "models")
    return tmp_path / "models"


def _edit(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    assert old in text
    path.write_text(text.replace(old, new), encoding="utf-8")


def test_the_report_the_model_and_the_dbt_contracts_agree() -> None:
    assert pbip.problems(PROJECT, MODELS) == []


def test_the_model_reads_only_published_gold_and_observability_tables() -> None:
    assert {schema for schema, _ in pbip.source_columns(PROJECT)} == {"gold", "observability"}


def test_the_model_never_imports_lineage_columns() -> None:
    columns = [c for cs in pbip.source_columns(PROJECT).values() for c in cs]
    assert not [c for c in columns if c.startswith("_")]


def test_a_multiline_measure_is_read_whole() -> None:
    table = pbip.load_table(TABLES / "Core banking reconciliation.tmdl")
    expression = table.measures["Core banking differences"]
    assert expression.count("SUM") == 3


def test_a_report_field_the_model_lost_is_reported(project: Path) -> None:
    _edit(
        project / "AmlCompliance.SemanticModel/definition/tables/Alert.tmdl", "measure 'PEP alerts'", "measure 'PEPs'"
    )
    assert "the report uses measure Alert[PEP alerts], which the model does not define" in pbip.problems(
        project, MODELS
    )


def test_a_dax_reference_to_a_missing_column_is_reported(project: Path) -> None:
    _edit(
        project / "AmlCompliance.SemanticModel/definition/tables/Alert.tmdl",
        "Alert[Is PEP] = TRUE",
        "Alert[PEP] = TRUE",
    )
    assert "measure Alert[PEP alerts] reads Alert[PEP], which the model does not define" in pbip.problems(
        project, MODELS
    )


def test_a_dax_reference_to_a_missing_measure_is_reported(project: Path) -> None:
    _edit(project / "AmlCompliance.SemanticModel/definition/tables/Alert.tmdl", "DIVIDE ( [Alerts]", "DIVIDE ( [Alert]")
    assert "measure Alert[Alerts per business day] reads measure [Alert], which the model does not define" in (
        pbip.problems(project, MODELS)
    )


def test_a_column_dropped_from_the_dbt_contract_is_reported(project: Path, models: Path) -> None:
    _edit(models / "gold/_gold.yml", "      - name: is_pep\n        data_type: boolean\n", "")
    assert "Alert[Is PEP] reads is_pep, not in the dbt contract" in pbip.problems(project, models)


def test_a_type_the_model_would_coerce_is_reported(project: Path, models: Path) -> None:
    _edit(
        models / "gold/_gold.yml",
        "      - name: transfer_count\n        data_type: bigint",
        "      - name: transfer_count\n        data_type: double",
    )
    assert "Alert[Transfers] is int64, its dbt contract type double loads as double" in pbip.problems(project, models)


def test_a_table_no_dbt_model_defines_is_reported(project: Path) -> None:
    _edit(project / "AmlCompliance.SemanticModel/definition/tables/Rule.tmdl", '"gold.aml_rule"', '"gold.aml_rules"')
    assert "table Rule reads gold.aml_rules, which no dbt model defines" in pbip.problems(project, MODELS)


def test_schema_violations_are_reported_with_their_location(tmp_path: Path) -> None:
    uri = "https://developer.microsoft.com/json-schemas/fabric/example/1.0.0/schema.json"
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["name"],
        "properties": {"position": {"type": "object", "properties": {"x": {"type": "number"}}}},
    }
    (tmp_path / "visual.json").write_text(json.dumps({"$schema": uri, "position": {"x": "left"}}))
    (tmp_path / "theme.json").write_text(json.dumps({"name": "no schema declared"}))
    fetched: list[str] = []

    def fetch(requested: str) -> Any:
        fetched.append(requested)
        return schema

    assert schemas.validate(tmp_path, fetch) == [
        "visual.json: (root): 'name' is a required property",
        "visual.json: position/x: 'left' is not of type 'number'",
    ]
    assert fetched == [uri]


def test_only_microsoft_schemas_are_fetched() -> None:
    with pytest.raises(ValueError, match="only Microsoft"):
        schemas.fetch_schema("https://example.com/schema.json")


class _Cursor:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, operation: str, params: Any = None) -> None:
        self.statements.append(operation)

    def fetchall(self) -> list[Any]:
        return [[0]]


def test_figures_cover_every_card_of_the_report() -> None:
    cards = {field for kind, entity, field in pbip.report_fields(PROJECT / "AmlCompliance.Report") if kind == "Measure"}
    computed = {f.measure for f in figures.compute(_Cursor())}
    card_measures = set()
    for path in (PROJECT / "AmlCompliance.Report/definition/pages").rglob("visual.json"):
        visual = json.loads(path.read_text())["visual"]
        if visual["visualType"] == "card":
            card_measures |= {
                p["field"]["Measure"]["Property"] for p in visual["query"]["queryState"]["Values"]["projections"]
            }
    assert card_measures <= computed <= cards


def test_figures_filter_the_dated_tables_on_the_period() -> None:
    cursor = _Cursor()
    figures.compute(cursor, date(2022, 9, 6), date(2022, 9, 10))
    dated = [s for s in cursor.statements if "business_date >=" in s]
    assert dated
    assert all("business_date <= date '2022-09-10'" in s for s in dated)
    assert not [s for s in cursor.statements if "sanctions_hit" in s and "business_date" in s]
