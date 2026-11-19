from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from aml_platform.sources import sanctions
from aml_platform.sources.landing import Manifest

UN = sanctions.SanctionsList("un", "https://un.test/list.xml", sanctions.UN.entry_tags, minimum_entries=2)
OFAC = sanctions.SanctionsList("ofac", "https://ofac.test/SDN.XML", sanctions.OFAC.entry_tags, minimum_entries=2)


def _serve(body: bytes) -> httpx.MockTransport:
    return httpx.MockTransport(lambda request: httpx.Response(200, content=body))


def _fetch(body: bytes, landing: Path, *, at: int = 1, allow_shrink: bool = False) -> sanctions.FetchResult:
    return sanctions.fetch(
        UN, landing, allow_shrink=allow_shrink, now=datetime(2026, 11, at, tzinfo=UTC), transport=_serve(body)
    )


def test_inspect_counts_entries_and_reads_publication(sanctions_dir: Path) -> None:
    assert sanctions.inspect(sanctions_dir / "un.xml", UN) == sanctions.ListSnapshot(3, "2026-11-12T00:00:04.690Z")
    assert sanctions.inspect(sanctions_dir / "ofac.xml", OFAC) == sanctions.ListSnapshot(3, "11/09/2026")


def test_declared_count_must_match_content(sanctions_dir: Path, tmp_path: Path) -> None:
    broken = tmp_path / "ofac.xml"
    broken.write_text((sanctions_dir / "ofac.xml").read_text().replace("<Record_Count>3", "<Record_Count>4"))
    with pytest.raises(sanctions.SanctionsValidationError, match="declares 4"):
        sanctions.inspect(broken, OFAC)


def test_too_few_entries_is_rejected(sanctions_dir: Path) -> None:
    strict = sanctions.SanctionsList("un", UN.url, UN.entry_tags, minimum_entries=500)
    with pytest.raises(sanctions.SanctionsValidationError, match="fewer than the minimum"):
        sanctions.inspect(sanctions_dir / "un.xml", strict)


def test_truncated_xml_is_never_landed(sanctions_dir: Path, tmp_path: Path) -> None:
    body = (sanctions_dir / "un.xml").read_bytes()[:-200]
    with pytest.raises(Exception, match=r"no element found|unclosed token|mismatched tag"):
        _fetch(body, tmp_path)
    assert not list((tmp_path / "un").iterdir())


def test_fetch_lands_file_and_manifest(sanctions_dir: Path, tmp_path: Path) -> None:
    result = _fetch((sanctions_dir / "un.xml").read_bytes(), tmp_path)

    assert result.changed
    assert result.path == tmp_path / "un" / "un_20261101T000000Z.xml"
    manifest = Manifest.read(Manifest.path_for(result.path))
    assert manifest.record_count == 3
    assert manifest.attributes["source_url"] == UN.url


def test_unchanged_list_is_not_landed_twice(sanctions_dir: Path, tmp_path: Path) -> None:
    body = (sanctions_dir / "un.xml").read_bytes()
    first = _fetch(body, tmp_path, at=1)
    second = _fetch(body, tmp_path, at=2)

    assert not second.changed
    assert second.path == first.path
    assert len(list((tmp_path / "un").glob("*.xml"))) == 1


def test_sharp_shrink_is_refused_unless_confirmed(sanctions_dir: Path, tmp_path: Path) -> None:
    full = (sanctions_dir / "un.xml").read_text()
    _fetch(full.encode(), tmp_path, at=1)
    start = full.index("<INDIVIDUAL>")
    shrunk = (full[:start] + full[full.index("</INDIVIDUAL>", start) + len("</INDIVIDUAL>") :]).encode()

    with pytest.raises(sanctions.SanctionsValidationError, match="shrank from 3 to 2"):
        _fetch(shrunk, tmp_path, at=2)
    assert _fetch(shrunk, tmp_path, at=3, allow_shrink=True).entry_count == 2


def test_listed_persons_skip_entities(sanctions_dir: Path) -> None:
    un = list(sanctions.listed_persons(sanctions_dir / "un.xml", sanctions.UN))
    ofac = list(sanctions.listed_persons(sanctions_dir / "ofac.xml", sanctions.OFAC))

    assert [(p.entry_id, p.names) for p in un] == [
        ("QDi.900", ("ABDUL", "KARIM", "HAMID")),
        ("CDi.900", ("JEAN", "DUPONT")),
    ]
    assert [(p.entry_id, p.names) for p in ofac] == [("200", ("Omar", "FAROUK")), ("300", ("Li", "WEI-MING"))]
