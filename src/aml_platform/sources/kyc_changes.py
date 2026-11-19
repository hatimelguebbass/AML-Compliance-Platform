"""Day-to-day maintenance of the core banking customer master.

Applies one batch of what a back office changes in a day: risk re-ratings after
KYC reviews, address moves, politically exposed person designations, accounts
that go dormant or are closed (their cards blocked with them), lost cards
blocked, and cards issued then cancelled the same day. Each operation commits on
its own, as separate business transactions would, so change data capture sees
the same sequence of commits a real day produces.

Rows are picked deterministically from the seed: the same seed on the same data
changes the same rows, another seed simulates another day.
"""

from __future__ import annotations

import logging
import random
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any

import psycopg
from psycopg import sql

from aml_platform.sources import reference as ref
from aml_platform.sources.corebanking import CARD_PRODUCTS, CARD_SCHEMES

log = logging.getLogger(__name__)

Operation = Callable[[psycopg.Cursor[Any], random.Random, int, int], int]


def _sample(column: str, table: str, condition: str) -> sql.Composed:
    # md5 of the key and the seed gives a stable pseudo-random order without loading ids client-side.
    return sql.SQL(
        "SELECT {column} FROM {table} WHERE {condition} ORDER BY md5({column}::text || %(seed)s) LIMIT %(n)s"
    ).format(column=sql.Identifier(column), table=sql.Identifier(table), condition=sql.SQL(condition))


def _risk_reviews(cursor: psycopg.Cursor[Any], _: random.Random, n: int, seed: int) -> int:
    cursor.execute(
        sql.SQL(
            "UPDATE customer SET risk_rating = CASE risk_rating WHEN 'LOW' THEN 'MEDIUM' ELSE 'HIGH' END"
            " WHERE customer_id IN ({})"
        ).format(_sample("customer_id", "customer", "risk_rating <> 'HIGH'")),
        {"seed": str(seed), "n": n},
    )
    return cursor.rowcount


def _address_moves(cursor: psycopg.Cursor[Any], rng: random.Random, n: int, seed: int) -> int:
    cursor.execute(_sample("customer_id", "customer", "residence_country = 'MA'"), {"seed": f"move{seed}", "n": n})
    moves = []
    for (customer_id,) in cursor.fetchall():
        city, _, postal_code, _ = rng.choice(ref.CITIES)
        address = f"{rng.randint(1, 250)} {rng.choice(ref.STREETS)}"
        moves.append((address, city, postal_code, customer_id))
    cursor.executemany(
        "UPDATE customer SET address_line = %s, city = %s, postal_code = %s WHERE customer_id = %s", moves
    )
    return len(moves)


def _pep_designations(cursor: psycopg.Cursor[Any], _: random.Random, n: int, seed: int) -> int:
    cursor.execute(
        sql.SQL("UPDATE customer SET is_pep = true, risk_rating = 'HIGH' WHERE customer_id IN ({})").format(
            _sample("customer_id", "customer", "customer_type = 'PERSON' AND NOT is_pep")
        ),
        {"seed": f"pep{seed}", "n": max(1, n // 10)},
    )
    return cursor.rowcount


def _dormant_accounts(cursor: psycopg.Cursor[Any], _: random.Random, n: int, seed: int) -> int:
    cursor.execute(
        sql.SQL("UPDATE account SET status = 'DORMANT' WHERE account_id IN ({})").format(
            _sample("account_id", "account", "status = 'ACTIVE'")
        ),
        {"seed": f"dormant{seed}", "n": n},
    )
    return cursor.rowcount


def _closed_accounts(cursor: psycopg.Cursor[Any], _: random.Random, n: int, seed: int) -> int:
    cursor.execute(
        sql.SQL(
            "UPDATE account SET status = 'CLOSED', closed_on = greatest(current_date, opened_on)"
            " WHERE account_id IN ({}) RETURNING account_id"
        ).format(_sample("account_id", "account", "status <> 'CLOSED'")),
        {"seed": f"closed{seed}", "n": n},
    )
    closed = [row[0] for row in cursor.fetchall()]
    cursor.execute("UPDATE card SET status = 'BLOCKED' WHERE account_id = ANY(%s) AND status = 'ACTIVE'", (closed,))
    return len(closed)


def _blocked_cards(cursor: psycopg.Cursor[Any], _: random.Random, n: int, seed: int) -> int:
    cursor.execute(
        sql.SQL("UPDATE card SET status = 'BLOCKED' WHERE card_id IN ({})").format(
            _sample("card_id", "card", "status = 'ACTIVE'")
        ),
        {"seed": f"blocked{seed}", "n": n},
    )
    return cursor.rowcount


def _cancelled_cards(cursor: psycopg.Cursor[Any], rng: random.Random, n: int, seed: int) -> int:
    """Cards issued by mistake and deleted the same day: the only deletes a card system allows."""
    cursor.execute(
        _sample("account_id", "account", "status = 'ACTIVE'"), {"seed": f"cancel{seed}", "n": max(1, n // 10)}
    )
    accounts = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT coalesce(max(card_id), 0) FROM card")
    row = cursor.fetchone()
    next_id = (row[0] if row else 0) + 1
    today = date.today()
    expires = date(today.year + 4, today.month, 1) - timedelta(days=1)
    issued = []
    for offset, account_id in enumerate(accounts):
        token = f"8{rng.randint(0, 10**15 - 1):015d}"
        cursor.execute(
            "INSERT INTO card (card_id, account_id, card_token, scheme, pan_last4, product, status, issued_on,"
            " expires_on) VALUES (%s, %s, %s, %s, %s, %s, 'ACTIVE', %s, %s)",
            (
                next_id + offset,
                account_id,
                token,
                rng.choice(CARD_SCHEMES),
                f"{rng.randint(0, 9999):04d}",
                CARD_PRODUCTS[0][0],
                today,
                expires,
            ),
        )
        issued.append(next_id + offset)
    cursor.connection.commit()
    cursor.execute("DELETE FROM card WHERE card_id = ANY(%s)", (issued,))
    return len(issued)


OPERATIONS: dict[str, Operation] = {
    "risk_reviews": _risk_reviews,
    "address_moves": _address_moves,
    "pep_designations": _pep_designations,
    "dormant_accounts": _dormant_accounts,
    "closed_accounts": _closed_accounts,
    "blocked_cards": _blocked_cards,
    "cancelled_cards": _cancelled_cards,
}


def apply(conninfo: str, *, per_operation: int, seed: int, schema: str = "core") -> dict[str, int]:
    """Apply one batch of changes; returns the number of rows each operation changed."""
    rng = random.Random(seed)
    applied: dict[str, int] = {}
    with psycopg.connect(conninfo) as connection, connection.cursor() as cursor:
        cursor.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        for name, operation in OPERATIONS.items():
            applied[name] = operation(cursor, rng, per_operation, seed)
            connection.commit()
            log.info("%s: %s rows", name, applied[name])
    return applied
