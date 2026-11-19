"""Acquisition of the IBM AML HI-Small files that back the payments feed.

The files are pinned by SHA-256: every downstream number (row counts, rule
precision and recall) is only reproducible if everyone starts from the same bytes.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

import httpx

from aml_platform.sources.landing import atomic_writer, sha256_of

log = logging.getLogger(__name__)

KAGGLE_DATASET = "ealtman2019/ibm-transactions-for-anti-money-laundering-aml"
KAGGLE_API = "https://www.kaggle.com/api/v1"


@dataclass(frozen=True)
class SourceFile:
    name: str
    sha256: str


TRANSACTIONS = SourceFile("HI-Small_Trans.csv", "b19d39f515523373f991b689c07e11e7b0b95c17a2c27a87d91584ae16c5b040")
ACCOUNTS = SourceFile("HI-Small_accounts.csv", "786808526e33cfc441212dd6fccda7edfc24172149bed59c6ef59b186836b014")
PATTERNS = SourceFile("HI-Small_Patterns.txt", "2c546b5ce6009e73851f0139af053cf845f08bf92f3bc82fe1eb937dec2ef39b")
FILES: tuple[SourceFile, ...] = (TRANSACTIONS, ACCOUNTS, PATTERNS)


class ChecksumMismatchError(RuntimeError):
    pass


class MissingCredentialsError(RuntimeError):
    pass


def kaggle_auth_header(env: dict[str, str] | None = None, home: Path | None = None) -> str:
    """Resolve Kaggle credentials the same way the official CLI does."""
    env = dict(os.environ) if env is None else env
    if token := env.get("KAGGLE_API_TOKEN"):
        return f"Bearer {token}"
    username, key = env.get("KAGGLE_USERNAME"), env.get("KAGGLE_KEY")
    if not (username and key):
        config_dir = Path(env["KAGGLE_CONFIG_DIR"]) if "KAGGLE_CONFIG_DIR" in env else (home or Path.home()) / ".kaggle"
        credentials = config_dir / "kaggle.json"
        if not credentials.is_file():
            raise MissingCredentialsError(
                "Kaggle credentials not found: set KAGGLE_API_TOKEN, KAGGLE_USERNAME/KAGGLE_KEY, "
                f"or create {credentials}; alternatively pass --from-dir with files downloaded manually"
            )
        payload = json.loads(credentials.read_text(encoding="utf-8"))
        username, key = payload["username"], payload["key"]
    basic = base64.b64encode(f"{username}:{key}".encode()).decode("ascii")
    return f"Basic {basic}"


def verify(path: Path, expected: SourceFile) -> None:
    actual = sha256_of(path)
    if actual != expected.sha256:
        raise ChecksumMismatchError(f"{path}: sha256 {actual} does not match pinned {expected.sha256}")


def _extract_if_zipped(download: Path, member: str, target: Path) -> None:
    # Kaggle serves large files compressed; the archive holds a single member.
    if zipfile.is_zipfile(download):
        with (
            zipfile.ZipFile(download) as archive,
            archive.open(member) as source,
            atomic_writer(target, "wb") as sink,
        ):
            shutil.copyfileobj(source, sink)
    else:
        with download.open("rb") as source, atomic_writer(target, "wb") as sink:
            shutil.copyfileobj(source, sink)


def download_from_kaggle(
    target_dir: Path,
    *,
    auth_header: str,
    transport: httpx.BaseTransport | None = None,
    files: tuple[SourceFile, ...] = FILES,
) -> list[Path]:
    target_dir.mkdir(parents=True, exist_ok=True)
    landed: list[Path] = []
    with httpx.Client(
        base_url=KAGGLE_API,
        headers={"Authorization": auth_header},
        follow_redirects=True,
        timeout=httpx.Timeout(60.0, read=300.0),
        transport=transport,
    ) as http:
        for source in files:
            target = target_dir / source.name
            if target.is_file() and sha256_of(target) == source.sha256:
                log.info("%s already present and verified", source.name)
                landed.append(target)
                continue
            log.info("downloading %s", source.name)
            with tempfile.NamedTemporaryFile(dir=target_dir, prefix=".download-", delete=False) as tmp:
                download = Path(tmp.name)
            try:
                with http.stream("GET", f"/datasets/download/{KAGGLE_DATASET}/{source.name}") as response:
                    response.raise_for_status()
                    with download.open("wb") as sink:
                        for chunk in response.iter_bytes():
                            sink.write(chunk)
                _extract_if_zipped(download, source.name, target)
            finally:
                download.unlink(missing_ok=True)
            verify(target, source)
            landed.append(target)
    return landed


def copy_from_directory(source_dir: Path, target_dir: Path, *, files: tuple[SourceFile, ...] = FILES) -> list[Path]:
    """Verify and stage files obtained outside this tool, e.g. through the Kaggle web UI."""
    target_dir.mkdir(parents=True, exist_ok=True)
    landed: list[Path] = []
    for source in files:
        origin = source_dir / source.name
        if not origin.is_file():
            raise FileNotFoundError(origin)
        verify(origin, source)
        target = target_dir / source.name
        if origin.resolve() != target.resolve():
            with origin.open("rb") as reader, atomic_writer(target, "wb") as writer:
                shutil.copyfileobj(reader, writer)
        landed.append(target)
    return landed
