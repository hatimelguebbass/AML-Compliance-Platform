from __future__ import annotations

import base64
import hashlib
import io
import json
import zipfile
from pathlib import Path

import httpx
import pytest

from aml_platform.sources import ibm_aml
from aml_platform.sources.landing import sha256_of

PAYLOAD = b"Timestamp,From Bank\n2022/09/01 00:20,010\n"


def _source(name: str = "HI-Small_Trans.csv", payload: bytes = PAYLOAD) -> ibm_aml.SourceFile:
    return ibm_aml.SourceFile(name, hashlib.sha256(payload).hexdigest())


def _zipped(name: str, payload: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, payload)
    return buffer.getvalue()


def test_bearer_token_takes_precedence() -> None:
    env = {"KAGGLE_API_TOKEN": "tok", "KAGGLE_USERNAME": "u", "KAGGLE_KEY": "k"}
    assert ibm_aml.kaggle_auth_header(env) == "Bearer tok"


def test_basic_auth_from_kaggle_json(tmp_path: Path) -> None:
    (tmp_path / ".kaggle").mkdir()
    (tmp_path / ".kaggle" / "kaggle.json").write_text(json.dumps({"username": "analyst", "key": "secret"}))

    header = ibm_aml.kaggle_auth_header({}, home=tmp_path)

    assert header == "Basic " + base64.b64encode(b"analyst:secret").decode()


def test_missing_credentials_point_to_manual_route(tmp_path: Path) -> None:
    with pytest.raises(ibm_aml.MissingCredentialsError, match="--from-dir"):
        ibm_aml.kaggle_auth_header({}, home=tmp_path)


@pytest.mark.parametrize("zipped", [False, True], ids=["plain", "zipped"])
def test_download_verifies_and_lands_file(tmp_path: Path, zipped: bool) -> None:
    source = _source()
    requested: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requested.append(request)
        body = _zipped(source.name, PAYLOAD) if zipped else PAYLOAD
        return httpx.Response(200, content=body)

    landed = ibm_aml.download_from_kaggle(
        tmp_path, auth_header="Bearer t", transport=httpx.MockTransport(handle), files=(source,)
    )

    assert landed == [tmp_path / source.name]
    assert (tmp_path / source.name).read_bytes() == PAYLOAD
    assert requested[0].url.path.endswith(f"/datasets/download/{ibm_aml.KAGGLE_DATASET}/{source.name}")
    assert requested[0].headers["Authorization"] == "Bearer t"
    assert sorted(p.name for p in tmp_path.iterdir()) == [source.name]


def test_verified_file_is_not_downloaded_again(tmp_path: Path) -> None:
    source = _source()
    (tmp_path / source.name).write_bytes(PAYLOAD)

    def handle(request: httpx.Request) -> httpx.Response:
        raise AssertionError("no request expected")

    ibm_aml.download_from_kaggle(tmp_path, auth_header="x", transport=httpx.MockTransport(handle), files=(source,))


def test_tampered_download_is_rejected(tmp_path: Path) -> None:
    source = _source()

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=PAYLOAD + b"extra\n")

    with pytest.raises(ibm_aml.ChecksumMismatchError):
        ibm_aml.download_from_kaggle(tmp_path, auth_header="x", transport=httpx.MockTransport(handle), files=(source,))


def test_copy_from_directory_checks_every_file(tmp_path: Path) -> None:
    origin, target = tmp_path / "downloads", tmp_path / "raw"
    origin.mkdir()
    (origin / "HI-Small_Trans.csv").write_bytes(PAYLOAD)

    landed = ibm_aml.copy_from_directory(origin, target, files=(_source(),))

    assert sha256_of(landed[0]) == _source().sha256
    with pytest.raises(ibm_aml.ChecksumMismatchError):
        ibm_aml.copy_from_directory(origin, target, files=(_source(payload=b"other"),))


def test_pinned_checksums_cover_the_three_files() -> None:
    assert {f.name for f in ibm_aml.FILES} == {"HI-Small_Trans.csv", "HI-Small_accounts.csv", "HI-Small_Patterns.txt"}
    assert all(len(f.sha256) == 64 for f in ibm_aml.FILES)
