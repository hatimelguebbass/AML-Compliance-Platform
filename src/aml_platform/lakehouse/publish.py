"""Publish the staging branch: merge ``etl`` into ``main``.

The pipeline calls this only after the audit step passed. The merge is atomic,
so readers of ``main`` see either the previous state or the whole new run.
"""

from __future__ import annotations

import argparse
import logging
import os

from aml_platform.lakehouse.bootstrap import STAGING_BRANCH
from aml_platform.lakehouse.nessie import NessieClient

log = logging.getLogger(__name__)


def publish(nessie_url: str, message: str) -> str:
    """Merge the staging branch into main and return main's new commit hash."""
    with NessieClient(nessie_url) as nessie:
        before = nessie.get_branch("main")
        if before is None or nessie.get_branch(STAGING_BRANCH) is None:
            raise SystemExit(f"branches main and {STAGING_BRANCH} must exist; run make bootstrap")
        after = nessie.merge(STAGING_BRANCH, "main", message=message)
    if after.hash == before.hash:
        log.info("nothing to publish: %s has no commit that main lacks", STAGING_BRANCH)
    else:
        log.info("published %s into main at %s", STAGING_BRANCH, after.hash[:12])
    return after.hash


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nessie-url", default=os.environ.get("NESSIE_URL", "http://localhost:19120"))
    parser.add_argument("--message", default="publish staging branch")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    publish(args.nessie_url, args.message)


if __name__ == "__main__":
    main()
