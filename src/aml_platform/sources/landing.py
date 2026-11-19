"""File delivery primitives shared by every file-based source.

A consumer must never pick up a half-written file. Data files are written under a
temporary name and renamed into place; for feeds without an in-band trailer, a
manifest written *after* the data file is the completeness signal.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import IO, Any

_CHUNK = 1 << 20


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


@contextmanager
def atomic_writer(path: Path, mode: str = "w", **open_kwargs: Any) -> Iterator[IO[Any]]:
    """Yield a handle on a temporary sibling of ``path``, renamed onto it on success."""
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.partial")
    try:
        with partial.open(mode, **open_kwargs) as handle:
            yield handle
            handle.flush()
            os.fsync(handle.fileno())
        partial.replace(path)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise


@dataclass(frozen=True)
class Manifest:
    file_name: str
    record_count: int
    sha256: str
    attributes: dict[str, str]

    @staticmethod
    def path_for(data_file: Path) -> Path:
        return data_file.with_name(f"{data_file.name}.manifest.json")

    def write_next_to(self, data_file: Path) -> Path:
        target = self.path_for(data_file)
        with atomic_writer(target, encoding="utf-8") as handle:
            json.dump(asdict(self), handle, indent=2, sort_keys=True)
            handle.write("\n")
        return target

    @classmethod
    def read(cls, path: Path) -> Manifest:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            file_name=payload["file_name"],
            record_count=int(payload["record_count"]),
            sha256=payload["sha256"],
            attributes=dict(payload.get("attributes", {})),
        )
