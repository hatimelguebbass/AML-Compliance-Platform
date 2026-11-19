from __future__ import annotations

import json
from dataclasses import fields
from pathlib import Path

import jsonschema
import pytest
import yaml

from aml_platform import contracts
from aml_platform.sources import cards, corebanking, payments

CONTRACT_FILES = sorted(contracts.CONTRACTS_DIR.glob("*.odcs.yaml"))


def test_contracts_exist() -> None:
    assert len(CONTRACT_FILES) == 4


@pytest.mark.parametrize("path", CONTRACT_FILES, ids=lambda p: p.name)
def test_contract_is_valid_odcs(path: Path) -> None:
    schema = json.loads((contracts.CONTRACTS_DIR / "schema" / "odcs-json-schema-v3.1.0.json").read_text())
    jsonschema.Draft201909Validator(schema).validate(yaml.safe_load(path.read_text()))


def test_contract_ids_are_unique() -> None:
    ids = [contracts.load(path).id for path in CONTRACT_FILES]
    assert len(ids) == len(set(ids))


def test_payments_feed_matches_contract() -> None:
    contract = contracts.load_by_id("payments-hub-transfers")
    assert contract.object("payment_transfer").column_names == payments.FEED_COLUMNS


def test_card_detail_record_matches_contract() -> None:
    contract = contracts.load_by_id("card-processor-transactions")
    assert contract.object("card_transaction").column_names == cards.DETAIL_FIELDS


@pytest.mark.parametrize(
    ("table", "row_type"),
    [("customer", corebanking.Customer), ("account", corebanking.Account), ("card", corebanking.Card)],
)
def test_corebanking_rows_match_contract(table: str, row_type: type) -> None:
    contract = contracts.load_by_id("corebanking-customer-master")
    loaded = {f.name for f in fields(row_type)}
    declared = contract.object(table).column_names
    # closed_on is part of the table but never set at account opening.
    assert loaded == set(declared) - {"closed_on"}


def test_unknown_contract_is_reported() -> None:
    with pytest.raises(KeyError):
        contracts.load_by_id("does-not-exist")


def test_ingestion_options_are_read() -> None:
    transfer = contracts.load_by_id("payments-hub-transfers").object("payment_transfer")
    by_name = {p.name: p for p in transfer.properties}
    assert by_name["booked_at"].format == "yyyy-MM-dd'T'HH:mm:ss"
    assert by_name["transfer_id"].pattern == "^TRF[0-9]{9}$"
    assert by_name["payment_format"].valid_values is not None
    assert "WIRE" in by_name["payment_format"].valid_values


def test_uniqueness_rules_keep_their_severity() -> None:
    card = contracts.load_by_id("card-processor-transactions").object("card_transaction")
    assert card.uniqueness == (contracts.UniquenessRule(("txn_ref",), "warning"),)


def test_primary_key_implies_uniqueness() -> None:
    entry = contracts.load_by_id("sanctions-lists").object("ofac_sdn_entry")
    assert entry.uniqueness == (contracts.UniquenessRule(("uid",), "error"),)


def test_nested_structure_is_read() -> None:
    individual = contracts.load_by_id("sanctions-lists").object("un_individual")
    alias = {p.name: p for p in individual.properties}["INDIVIDUAL_ALIAS"]
    assert alias.items is not None
    assert [p.name for p in alias.items.properties] == ["QUALITY", "ALIAS_NAME"]
