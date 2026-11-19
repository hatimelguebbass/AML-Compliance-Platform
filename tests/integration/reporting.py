"""Connections the way Power BI makes them: Trino's HTTPS listener, a user and its password."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import trino
from trino.auth import BasicAuthentication

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
PASSWORDS = {
    "bi": os.environ.get("TRINO_BI_PASSWORD", "bi-local"),
    "compliance": os.environ.get("TRINO_COMPLIANCE_PASSWORD", "compliance-local"),
    "analyst": os.environ.get("TRINO_ANALYST_PASSWORD", "analyst-local"),
}


def server_certificate(directory: Path) -> Path:
    """The certificate trino-init generated, copied out of the container to verify the server with."""
    target = directory / "trino.crt"
    subprocess.run(
        ["docker", "compose", "cp", "trino:/etc/trino/security/trino.crt", str(target)],
        check=True,
        capture_output=True,
    )
    return target


def https_query(user: str, password: str, sql: str, certificate: Path) -> list[tuple[object, ...]]:
    connection = trino.dbapi.connect(
        host=TRINO_HOST,
        port=8443,
        user=user,
        auth=BasicAuthentication(user, password),
        http_scheme="https",
        verify=str(certificate),
    )
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        connection.close()
