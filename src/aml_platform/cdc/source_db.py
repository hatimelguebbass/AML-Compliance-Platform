"""Database side of change data capture: capture role, grants and publication.

Applied by the schema owner, so the role Debezium logs in with only needs to
replicate and to read the captured tables for the initial snapshot. Every
statement converges to the declared state and can run on each deployment.
"""

from __future__ import annotations

import logging
import time

import psycopg
from psycopg import sql

from aml_platform.cdc.topics import CdcSource

log = logging.getLogger(__name__)


class SourceNotReadyError(RuntimeError):
    """The captured tables do not exist yet."""


def prepare(conninfo: str, source: CdcSource, *, role: str, password: str) -> None:
    tables = [sql.Identifier(source.schema, table) for table in source.tables]
    with psycopg.connect(conninfo, autocommit=True) as connection:
        found = connection.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = %s AND table_name = ANY(%s)",
            (source.schema, list(source.tables)),
        ).fetchone()
        if found is None or found[0] != len(source.tables):
            raise SourceNotReadyError(
                f"{source.schema} lacks some of {', '.join(source.tables)}: load the core banking data first"
            )

        exists = connection.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone()
        verb = sql.SQL("ALTER") if exists else sql.SQL("CREATE")
        connection.execute(
            sql.SQL("{} ROLE {} WITH LOGIN REPLICATION PASSWORD {}").format(
                verb, sql.Identifier(role), sql.Literal(password)
            )
        )
        connection.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(sql.Identifier(source.schema), sql.Identifier(role))
        )
        connection.execute(sql.SQL("GRANT SELECT ON {} TO {}").format(sql.SQL(", ").join(tables), sql.Identifier(role)))

        # The default identity logs only the primary key of a deleted or updated row, and Debezium fills
        # the other NOT NULL columns of the old image with zero values: a deleted card would show an
        # empty token. A full identity puts the real row in every delete, which the KYC audit trail needs.
        for table in tables:
            connection.execute(sql.SQL("ALTER TABLE {} REPLICA IDENTITY FULL").format(table))

        publication = sql.Identifier(source.publication_name)
        has_publication = connection.execute(
            "SELECT 1 FROM pg_publication WHERE pubname = %s", (source.publication_name,)
        ).fetchone()
        statement = "ALTER PUBLICATION {} SET TABLE {}" if has_publication else "CREATE PUBLICATION {} FOR TABLE {}"
        connection.execute(sql.SQL(statement).format(publication, sql.SQL(", ").join(tables)))
    log.info(
        "publication %s covers %s", source.publication_name, ", ".join(f"{source.schema}.{t}" for t in source.tables)
    )


def drop(conninfo: str, source: CdcSource, timeout: float = 60.0) -> None:
    """Remove the slot and publication of a stopped connector, so the database stops retaining WAL for it."""
    deadline = time.monotonic() + timeout
    with psycopg.connect(conninfo, autocommit=True) as connection:
        while True:
            slot = connection.execute(
                "SELECT active FROM pg_replication_slots WHERE slot_name = %s", (source.slot_name,)
            ).fetchone()
            if slot is None:
                break
            if not slot[0]:
                connection.execute("SELECT pg_drop_replication_slot(%s)", (source.slot_name,))
                break
            if time.monotonic() > deadline:
                raise TimeoutError(f"replication slot {source.slot_name} still in use")
            time.sleep(1)
        connection.execute(sql.SQL("DROP PUBLICATION IF EXISTS {}").format(sql.Identifier(source.publication_name)))


def slot_lag_bytes(conninfo: str, source: CdcSource) -> int | None:
    """WAL the database retains because the connector has not confirmed it yet; None without a slot."""
    with psycopg.connect(conninfo) as connection:
        row = connection.execute(
            "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint"
            " FROM pg_replication_slots WHERE slot_name = %s",
            (source.slot_name,),
        ).fetchone()
    return None if row is None else int(row[0])
