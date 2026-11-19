"""Validation of the Power BI project's JSON files against the schemas they declare.

Microsoft publishes the PBIR and PBIP schemas; each file names its own in ``$schema``. They are
fetched on use rather than vendored, so a file moved to a newer schema version is validated against
that version without anyone refreshing a copy.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
from jsonschema import Draft7Validator
from referencing import Registry, Resource

_SCHEMA_HOST = "https://developer.microsoft.com/json-schemas/"

Fetch = Callable[[str], Any]


def fetch_schema(uri: str) -> Any:
    if not uri.startswith(_SCHEMA_HOST):
        raise ValueError(f"{uri}: only Microsoft's published schemas are fetched")
    response = httpx.get(uri, timeout=30, follow_redirects=True)
    response.raise_for_status()
    return response.json()


def validate(project_dir: Path, fetch: Fetch = fetch_schema) -> list[str]:
    """Every schema violation of the JSON files under ``project_dir`` that declare a schema."""
    cache: dict[str, Any] = {}

    def retrieve(uri: str) -> Resource[Any]:
        if uri not in cache:
            cache[uri] = fetch(uri)
        return Resource.from_contents(cache[uri])

    registry: Registry[Any] = Registry(retrieve=retrieve)  # type: ignore[call-arg]
    found: list[str] = []
    files = sorted(
        p for p in project_dir.rglob("*") if p.suffix in {".json", ".pbip", ".pbir", ".pbism"} or p.name == ".platform"
    )
    for path in files:
        document = json.loads(path.read_text(encoding="utf-8"))
        uri = document.get("$schema") if isinstance(document, dict) else None
        if not uri:
            continue
        validator = Draft7Validator(retrieve(uri).contents, registry=registry)
        for error in validator.iter_errors(document):
            location = "/".join(str(part) for part in error.absolute_path) or "(root)"
            found.append(f"{path.relative_to(project_dir)}: {location}: {error.message}")
    return found
