"""Branch operations on the Nessie catalog used by the write-audit-publish flow.

Pipelines write to a staging branch, run audits there, and only then merge into
``main``. Every mutating call passes the expected commit hash, so a concurrent
writer makes the operation fail instead of silently overwriting history.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import httpx


class NessieError(RuntimeError):
    """Nessie rejected an operation or returned an unexpected response."""


class ReferenceConflictError(NessieError):
    """The reference moved since it was read (HTTP 409)."""


@dataclass(frozen=True)
class Branch:
    name: str
    hash: str


class NessieClient:
    def __init__(self, base_url: str, *, timeout: float = 30.0, transport: httpx.BaseTransport | None = None) -> None:
        self._http = httpx.Client(
            base_url=f"{base_url.rstrip('/')}/api/v2",
            timeout=timeout,
            transport=transport,
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> NessieClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def get_branch(self, name: str) -> Branch | None:
        response = self._http.get(f"/trees/{name}")
        if response.status_code == httpx.codes.NOT_FOUND:
            return None
        payload = self._json(response)
        reference = payload["reference"]
        if reference["type"] != "BRANCH":
            raise NessieError(f"reference {name!r} is a {reference['type']}, not a branch")
        return Branch(reference["name"], reference["hash"])

    def create_branch(self, name: str, source: Branch) -> Branch:
        response = self._http.post(
            "/trees",
            params={"name": name, "type": "BRANCH"},
            json={"type": "BRANCH", "name": source.name, "hash": source.hash},
        )
        reference = self._json(response)["reference"]
        return Branch(reference["name"], reference["hash"])

    def reset_branch(self, branch: Branch, target: Branch) -> Branch:
        """Point ``branch`` at ``target``'s commit, discarding unmerged work on it."""
        response = self._http.put(
            f"/trees/{branch.name}@{branch.hash}",
            params={"type": "BRANCH"},
            json={"type": "BRANCH", "name": target.name, "hash": target.hash},
        )
        reference = self._json(response)["reference"]
        return Branch(reference["name"], reference["hash"])

    def ensure_branch_at(self, name: str, source_name: str = "main") -> Branch:
        """Return branch ``name`` freshly aligned with ``source_name``.

        Staging branches are disposable: each run starts from the published state,
        so a failed previous run can never leak into the next one.
        """
        source = self._require(source_name)
        existing = self.get_branch(name)
        if existing is None:
            return self.create_branch(name, source)
        if existing.hash == source.hash:
            return existing
        return self.reset_branch(existing, source)

    def merge(self, source_name: str, target_name: str = "main", *, message: str) -> Branch:
        source = self._require(source_name)
        target = self._require(target_name)
        response = self._http.post(
            f"/trees/{target.name}@{target.hash}/history/merge",
            json={
                "fromRefName": source.name,
                "fromHash": source.hash,
                "message": message,
            },
        )
        self._json(response)
        return self._require(target_name)

    def delete_branch(self, branch: Branch) -> None:
        response = self._http.delete(f"/trees/{branch.name}@{branch.hash}", params={"type": "BRANCH"})
        self._json(response)

    def _require(self, name: str) -> Branch:
        branch = self.get_branch(name)
        if branch is None:
            raise NessieError(f"branch {name!r} does not exist")
        return branch

    @staticmethod
    def _json(response: httpx.Response) -> dict[str, Any]:
        if response.status_code == httpx.codes.CONFLICT:
            raise ReferenceConflictError(response.text)
        if response.is_error:
            request = response.request
            raise NessieError(f"{request.method} {request.url} -> {response.status_code}: {response.text}")
        return cast(dict[str, Any], response.json())
