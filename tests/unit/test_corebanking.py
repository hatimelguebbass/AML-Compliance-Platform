from __future__ import annotations

import re
from collections import Counter
from dataclasses import replace
from pathlib import Path

import pytest

from aml_platform.sources import corebanking
from aml_platform.sources.sanctions import ListedPerson

LISTED = [
    ListedPerson("un", "QDi.900", ("ABDUL", "KARIM", "HAMID")),
    ListedPerson("ofac", "200", ("Omar", "FAROUK")),
    ListedPerson("ofac", "201", ("X",)),
]
NO_DEFECTS = corebanking.GenerationConfig(
    duplicate_rate_person=0, duplicate_rate_organization=0, sanctioned_customers=0
)


def _generate(ibm_dir: Path, config: corebanking.GenerationConfig = NO_DEFECTS) -> corebanking.CoreBankingData:
    return corebanking.generate(
        corebanking.read_ibm_accounts(ibm_dir / "HI-Small_accounts.csv"),
        corebanking.account_currencies(ibm_dir / "HI-Small_Trans.csv"),
        LISTED,
        config,
    )


def test_every_ibm_account_belongs_to_exactly_one_customer(ibm_dir: Path) -> None:
    data = _generate(ibm_dir, replace(NO_DEFECTS, duplicate_rate_person=1.0, duplicate_rate_organization=1.0))
    keys = Counter((a.bank_id, a.account_number) for a in data.accounts)

    assert len(keys) == 9
    assert set(keys.values()) == {1}
    assert {a.customer_id for a in data.accounts} <= {c.customer_id for c in data.customers}


def test_customer_type_follows_ibm_entity(ibm_dir: Path) -> None:
    by_id = {c.customer_id: c for c in _generate(ibm_dir).customers}
    types = [by_id[cid].customer_type for cid in sorted(by_id)]
    assert types == ["PERSON", "ORGANIZATION", "PERSON", "ORGANIZATION", "PERSON", "PERSON"]


def test_bank_ids_are_normalised_and_currency_comes_from_payments(ibm_dir: Path) -> None:
    accounts = {(a.bank_id, a.account_number): a for a in _generate(ibm_dir).accounts}

    assert accounts["10", "8000AAA10"].currency == "USD"
    assert accounts["3208", "8000CCC31"].currency == "XBT"
    assert accounts["10", "8000AAA11"].currency == "CHF"
    dormant = accounts["10", "8000AAA12"]
    assert (dormant.status, dormant.currency) == ("DORMANT", "MAD")


def test_rib_carries_a_valid_mod97_key(ibm_dir: Path) -> None:
    for account in _generate(ibm_dir).accounts:
        assert re.fullmatch(r"\d{24}", account.rib)
        assert int(account.rib) % 97 == 0
        assert account.rib.startswith(corebanking.BANK_CODE)


def test_generation_is_deterministic(ibm_dir: Path) -> None:
    assert _generate(ibm_dir) == _generate(ibm_dir)
    other_seed = _generate(ibm_dir, replace(NO_DEFECTS, seed=7))
    assert other_seed.customers != _generate(ibm_dir).customers


def test_person_identity_formats(ibm_dir: Path) -> None:
    for customer in _generate(ibm_dir).customers:
        if customer.id_document_type == "CIN":
            assert re.fullmatch(r"[A-Z]{1,2}\d{4,6}", customer.id_document_number or "")
        if customer.customer_type == "ORGANIZATION":
            assert re.fullmatch(r"\d{15}", customer.tax_id or "")
        assert customer.customer_since <= corebanking.AS_OF


def test_duplicates_share_a_party_and_split_its_accounts(ibm_dir: Path) -> None:
    data = _generate(ibm_dir, replace(NO_DEFECTS, duplicate_rate_person=1.0, duplicate_rate_organization=1.0))
    roles = {cid: (party, role) for cid, party, role in data.parties}
    duplicates = [cid for cid, (_, role) in roles.items() if role == "duplicate"]

    # Only parties with several accounts can be split: entities 1 (three accounts) and 2 (two).
    assert {roles[cid][0] for cid in duplicates} == {"800E00001", "800E00002"}
    for cid in duplicates:
        party = roles[cid][0]
        owners = {a.customer_id for a in data.accounts if roles[a.customer_id][0] == party}
        assert len(owners) == 2


def test_sanctioned_names_are_planted_on_single_record_persons(ibm_dir: Path) -> None:
    data = _generate(ibm_dir, replace(NO_DEFECTS, sanctioned_customers=2))
    by_id = {c.customer_id: c for c in data.customers}

    assert len(data.sanctioned) == 2
    for customer_id, list_name, entry_id, listed_name in data.sanctioned:
        customer = by_id[customer_id]
        assert customer.customer_type == "PERSON"
        assert (list_name, entry_id) in {("un", "QDi.900"), ("ofac", "200")}
        planted = f"{customer.first_name} {customer.last_name}".upper().split()
        assert set(planted) <= set(listed_name.upper().split())


def test_cannot_plant_more_sanctions_than_usable_names(ibm_dir: Path) -> None:
    with pytest.raises(ValueError, match="usable"):
        _generate(ibm_dir, replace(NO_DEFECTS, sanctioned_customers=3))


def test_cards_only_on_current_accounts_of_persons(ibm_dir: Path) -> None:
    data = _generate(ibm_dir, replace(NO_DEFECTS, card_rate=1.0))
    accounts = {a.account_id: a for a in data.accounts}
    customers = {c.customer_id: c for c in data.customers}

    assert data.cards
    for card in data.cards:
        account = accounts[card.account_id]
        assert account.account_type == "CURRENT"
        assert customers[account.customer_id].customer_type == "PERSON"
        assert re.fullmatch(r"9\d{15}", card.card_token)
        assert card.expires_on > card.issued_on
