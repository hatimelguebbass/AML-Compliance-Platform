from __future__ import annotations

import json
from collections.abc import Callable

import httpx
import pytest

from aml_platform.lakehouse.nessie import Branch, NessieClient, NessieError, ReferenceConflictError

MAIN = Branch("main", "a" * 64)
ETL = Branch("etl", "b" * 64)

Handler = Callable[[httpx.Request], httpx.Response]


def _reference(branch: Branch) -> dict[str, object]:
    return {"reference": {"type": "BRANCH", "name": branch.name, "hash": branch.hash}}


def _client(handler: Handler) -> NessieClient:
    return NessieClient("http://nessie:19120", transport=httpx.MockTransport(handler))


def _router(branches: dict[str, Branch], calls: list[httpx.Request]) -> Handler:
    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        path = request.url.path.removeprefix("/api/v2")
        if request.method == "GET":
            name = path.removeprefix("/trees/")
            if name in branches:
                return httpx.Response(200, json=_reference(branches[name]))
            return httpx.Response(404, json={"message": "not found"})
        if request.method == "POST" and path == "/trees":
            name = request.url.params["name"]
            source = json.loads(request.content)
            branches[name] = Branch(name, source["hash"])
            return httpx.Response(200, json=_reference(branches[name]))
        if request.method == "PUT":
            name, _, _ = path.removeprefix("/trees/").partition("@")
            target = json.loads(request.content)
            branches[name] = Branch(name, target["hash"])
            return httpx.Response(200, json=_reference(branches[name]))
        return httpx.Response(500)

    return handle


def test_ensure_branch_creates_missing_branch_from_main() -> None:
    branches = {"main": MAIN}
    calls: list[httpx.Request] = []
    with _client(_router(branches, calls)) as client:
        branch = client.ensure_branch_at("etl")

    assert branch == Branch("etl", MAIN.hash)
    create = next(c for c in calls if c.method == "POST")
    assert create.url.params["type"] == "BRANCH"
    assert json.loads(create.content)["hash"] == MAIN.hash


def test_ensure_branch_resets_diverged_branch_with_optimistic_lock() -> None:
    branches = {"main": MAIN, "etl": ETL}
    calls: list[httpx.Request] = []
    with _client(_router(branches, calls)) as client:
        branch = client.ensure_branch_at("etl")

    assert branch.hash == MAIN.hash
    reset = next(c for c in calls if c.method == "PUT")
    assert reset.url.path.endswith(f"/trees/etl@{ETL.hash}")


def test_ensure_branch_is_noop_when_already_aligned() -> None:
    branches = {"main": MAIN, "etl": Branch("etl", MAIN.hash)}
    calls: list[httpx.Request] = []
    with _client(_router(branches, calls)) as client:
        client.ensure_branch_at("etl")

    assert all(c.method == "GET" for c in calls)


def test_merge_pins_both_hashes() -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.method == "GET":
            name = request.url.path.rsplit("/", 1)[-1]
            return httpx.Response(200, json=_reference({"main": MAIN, "etl": ETL}[name]))
        return httpx.Response(200, json={"resultantTargetHash": "c" * 64})

    with _client(handle) as client:
        client.merge("etl", message="publish run 42")

    merge = next(r for r in seen if r.method == "POST")
    assert merge.url.path == f"/api/v2/trees/main@{MAIN.hash}/history/merge"
    body = json.loads(merge.content)
    assert body == {"fromRefName": "etl", "fromHash": ETL.hash, "message": "publish run 42"}


def test_conflict_surfaces_as_dedicated_error() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            name = request.url.path.rsplit("/", 1)[-1]
            return httpx.Response(200, json=_reference({"main": MAIN, "etl": ETL}[name]))
        return httpx.Response(409, json={"message": "hash mismatch"})

    with _client(handle) as client, pytest.raises(ReferenceConflictError):
        client.merge("etl", message="publish")


def test_tag_is_not_accepted_as_branch() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"reference": {"type": "TAG", "name": "release", "hash": "d" * 64}})

    with _client(handle) as client, pytest.raises(NessieError, match="not a branch"):
        client.get_branch("release")
