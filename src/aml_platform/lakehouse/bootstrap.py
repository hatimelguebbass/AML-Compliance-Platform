"""Create the lakehouse namespaces on ``main``.

Runs once per environment and is safe to re-run. It also gives ``main`` its first
commit: Nessie cannot merge a branch back into a target that has no history yet.
"""

from __future__ import annotations

import argparse
import logging
import os

import httpx

from aml_platform.lakehouse.nessie import NessieClient, NessieError

NAMESPACES: tuple[str, ...] = ("bronze", "silver", "restricted", "reference", "gold", "audit", "observability")
STAGING_BRANCH = "etl"

log = logging.getLogger(__name__)


def ensure_namespace(http: httpx.Client, ref: str, namespace: str) -> bool:
    """Create ``namespace`` on ``ref``. Returns False when it already existed."""
    response = http.post(f"/iceberg/v1/{ref}/namespaces", json={"namespace": [namespace], "properties": {}})
    if response.status_code == httpx.codes.CONFLICT:
        return False
    if response.is_error:
        raise NessieError(f"creating namespace {namespace!r} on {ref!r} failed: {response.status_code} {response.text}")
    return True


def bootstrap(nessie_url: str, *, transport: httpx.BaseTransport | None = None) -> None:
    with httpx.Client(base_url=nessie_url.rstrip("/"), timeout=30.0, transport=transport) as http:
        for namespace in NAMESPACES:
            created = ensure_namespace(http, "main", namespace)
            log.info("namespace %s %s", namespace, "created" if created else "already present")
    with NessieClient(nessie_url, transport=transport) as nessie:
        branch = nessie.ensure_branch_at(STAGING_BRANCH)
        log.info("staging branch %s aligned with main at %s", branch.name, branch.hash[:12])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nessie-url", default=os.environ.get("NESSIE_URL", "http://localhost:19120"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    bootstrap(args.nessie_url)


if __name__ == "__main__":
    main()
