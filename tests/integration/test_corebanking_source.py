"""The core banking schema loads, enforces its constraints and matches its contract."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

from aml_platform import contracts
from aml_platform.sources import cards, corebanking, kyc_changes

pytestmark = pytest.mark.integration

CONNINFO = psycopg.conninfo.make_conninfo(
    host=os.environ.get("SOURCE_DB_HOST", "localhost"),
    port=os.environ.get("SOURCE_DB_PORT", "5432"),
    dbname=os.environ.get("SOURCE_DB_NAME", "corebanking"),
    user=os.environ.get("SOURCE_DB_USER", "corebanking"),
    password=os.environ.get("SOURCE_DB_PASSWORD", "corebanking-local"),
)


@pytest.fixture
def schema() -> Iterator[str]:
    name = f"core_test_{uuid.uuid4().hex[:8]}"
    yield name
    with psycopg.connect(CONNINFO, autocommit=True) as connection:
        connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(name)))


@pytest.fixture
def data(ibm_dir: Path) -> corebanking.CoreBankingData:
    return corebanking.generate(
        corebanking.read_ibm_accounts(ibm_dir / "HI-Small_accounts.csv"),
        corebanking.account_currencies(ibm_dir / "HI-Small_Trans.csv"),
        [],
        corebanking.GenerationConfig(duplicate_rate_person=1.0, card_rate=1.0, sanctioned_customers=0),
    )


def _columns(schema: str, table: str) -> tuple[str, ...]:
    with psycopg.connect(CONNINFO) as connection:
        rows = connection.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (schema, table),
        ).fetchall()
    return tuple(row[0] for row in rows)


def test_tables_match_contract(schema: str, data: corebanking.CoreBankingData) -> None:
    corebanking.load(CONNINFO, data, schema=schema)
    contract = contracts.load_by_id("corebanking-customer-master")

    for table in ("customer", "account", "card"):
        assert _columns(schema, table) == contract.object(table).column_names


def test_reload_requires_explicit_replace(schema: str, data: corebanking.CoreBankingData) -> None:
    corebanking.load(CONNINFO, data, schema=schema)

    with pytest.raises(corebanking.TargetNotEmptyError):
        corebanking.load(CONNINFO, data, schema=schema)
    corebanking.load(CONNINFO, data, schema=schema, replace=True)

    with psycopg.connect(CONNINFO) as connection:
        count = connection.execute(sql.SQL("SELECT count(*) FROM {}.account").format(sql.Identifier(schema))).fetchone()
    assert count == (9,)


def test_updated_at_is_maintained_by_the_database(schema: str, data: corebanking.CoreBankingData) -> None:
    corebanking.load(CONNINFO, data, schema=schema)
    customer = sql.Identifier(schema, "customer")

    with psycopg.connect(CONNINFO) as connection:
        before = connection.execute(sql.SQL("SELECT updated_at FROM {} WHERE customer_id = 1000001").format(customer))
        original = before.fetchone()
        connection.execute(sql.SQL("UPDATE {} SET risk_rating = 'HIGH' WHERE customer_id = 1000001").format(customer))
        after = connection.execute(sql.SQL("SELECT updated_at FROM {} WHERE customer_id = 1000001").format(customer))
        assert after.fetchone() != original


def test_constraints_reject_inconsistent_rows(schema: str, data: corebanking.CoreBankingData) -> None:
    person = next(c for c in data.customers if c.customer_type == "PERSON")
    broken = replace(data, customers=[replace(person, last_name=None)], accounts=[], cards=[])

    with pytest.raises(psycopg.errors.CheckViolation):
        corebanking.load(CONNINFO, broken, schema=schema)


def test_card_holders_feed_the_processor_files(schema: str, data: corebanking.CoreBankingData) -> None:
    corebanking.load(CONNINFO, data, schema=schema)

    holders = cards.load_card_holders(CONNINFO, schema)

    assert len(holders) == sum(1 for card in data.cards if card.status == "ACTIVE")
    assert len({h.card_token for h in holders}) == len(holders)


def test_kyc_changes_respect_the_schema_constraints(schema: str, data: corebanking.CoreBankingData) -> None:
    corebanking.load(CONNINFO, data, schema=schema)
    applied = kyc_changes.apply(CONNINFO, per_operation=3, seed=5, schema=schema)
    assert applied["risk_reviews"] == 3
    assert applied["closed_accounts"] == 3
    assert applied["cancelled_cards"] == 1

    with psycopg.connect(CONNINFO) as connection:
        connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        cards_on_closed_accounts = connection.execute(
            "SELECT count(*) FROM card JOIN account USING (account_id)"
            " WHERE account.status = 'CLOSED' AND card.status = 'ACTIVE'"
        ).fetchone()
        card_count = connection.execute("SELECT count(*) FROM card").fetchone()
    assert cards_on_closed_accounts == (0,)
    assert card_count == (len(data.cards),)
