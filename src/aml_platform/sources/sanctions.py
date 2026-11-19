"""Download of the UN Security Council consolidated list and the OFAC SDN list.

A sanctions list that silently shrinks or arrives empty disables screening without
any error downstream, so every download is validated before it is landed: the XML
must parse, the entry count must be plausible, and it must not drop sharply
against the previously landed version.
"""

from __future__ import annotations

import logging
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from xml.etree.ElementTree import Element

import httpx
from defusedxml.ElementTree import iterparse

from aml_platform.sources.landing import Manifest, sha256_of

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SanctionsList:
    name: str
    url: str
    entry_tags: frozenset[str]
    minimum_entries: int


UN = SanctionsList(
    name="un",
    url="https://scsanctions.un.org/resources/xml/en/consolidated.xml",
    entry_tags=frozenset({"INDIVIDUAL", "ENTITY"}),
    minimum_entries=500,
)
OFAC = SanctionsList(
    name="ofac",
    url="https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML",
    entry_tags=frozenset({"sdnEntry"}),
    minimum_entries=5_000,
)
LISTS: dict[str, SanctionsList] = {UN.name: UN, OFAC.name: OFAC}

# Accept at most this relative drop in entries between two consecutive downloads.
MAX_SHRINK = 0.10


class SanctionsValidationError(RuntimeError):
    pass


@dataclass(frozen=True)
class ListSnapshot:
    entry_count: int
    publication: str


@dataclass(frozen=True)
class ListedPerson:
    list_name: str
    entry_id: str
    names: tuple[str, ...]


@dataclass(frozen=True)
class FetchResult:
    path: Path
    entry_count: int
    changed: bool


def _local(tag: str) -> str:
    return tag.rpartition("}")[2]


def _text(element: Element, child: str) -> str:
    for node in element:
        if _local(node.tag) == child:
            return (node.text or "").strip()
    return ""


def inspect(path: Path, sanctions_list: SanctionsList) -> ListSnapshot:
    count = 0
    publication = ""
    declared: int | None = None
    for event, element in iterparse(path, events=("start", "end")):
        tag = _local(element.tag)
        if event == "start":
            if tag == "CONSOLIDATED_LIST":
                publication = element.get("dateGenerated", "")
            continue
        if tag in sanctions_list.entry_tags:
            count += 1
            element.clear()
        elif tag == "Publish_Date":
            publication = (element.text or "").strip()
        elif tag == "Record_Count":
            declared = int((element.text or "0").strip())
    if declared is not None and declared != count:
        raise SanctionsValidationError(f"{path.name}: file declares {declared} entries but contains {count}")
    if count < sanctions_list.minimum_entries:
        raise SanctionsValidationError(
            f"{path.name}: {count} entries, fewer than the minimum {sanctions_list.minimum_entries}"
        )
    if not publication:
        raise SanctionsValidationError(f"{path.name}: publication date missing")
    return ListSnapshot(count, publication)


def latest_manifest(list_dir: Path) -> Manifest | None:
    manifests = sorted(list_dir.glob("*.xml.manifest.json"))
    return Manifest.read(manifests[-1]) if manifests else None


def fetch(
    sanctions_list: SanctionsList,
    landing_dir: Path,
    *,
    allow_shrink: bool = False,
    now: datetime | None = None,
    transport: httpx.BaseTransport | None = None,
) -> FetchResult:
    list_dir = landing_dir / sanctions_list.name
    list_dir.mkdir(parents=True, exist_ok=True)
    retrieved_at = (now or datetime.now(UTC)).strftime("%Y%m%dT%H%M%SZ")
    with tempfile.NamedTemporaryFile(dir=list_dir, prefix=".download-", suffix=".xml", delete=False) as tmp:
        download = Path(tmp.name)
    try:
        with (
            httpx.Client(follow_redirects=True, timeout=httpx.Timeout(30.0, read=120.0), transport=transport) as http,
            http.stream("GET", sanctions_list.url) as response,
        ):
            response.raise_for_status()
            with download.open("wb") as sink:
                for chunk in response.iter_bytes():
                    sink.write(chunk)
        snapshot = inspect(download, sanctions_list)
        digest = sha256_of(download)

        previous = latest_manifest(list_dir)
        if previous is not None and previous.sha256 == digest:
            log.info("%s list unchanged since %s", sanctions_list.name, previous.file_name)
            return FetchResult(list_dir / previous.file_name, previous.record_count, changed=False)
        if (
            previous is not None
            and not allow_shrink
            and snapshot.entry_count < previous.record_count * (1 - MAX_SHRINK)
        ):
            raise SanctionsValidationError(
                f"{sanctions_list.name} list shrank from {previous.record_count} to {snapshot.entry_count} entries; "
                "rerun with --allow-shrink after confirming the change with the publisher"
            )

        target = list_dir / f"{sanctions_list.name}_{retrieved_at}.xml"
        download.replace(target)
        Manifest(
            file_name=target.name,
            record_count=snapshot.entry_count,
            sha256=digest,
            attributes={
                "list": sanctions_list.name,
                "publication": snapshot.publication,
                "retrieved_at": retrieved_at,
                "source_url": sanctions_list.url,
            },
        ).write_next_to(target)
        log.info(
            "%s list landed: %s entries, published %s", sanctions_list.name, snapshot.entry_count, snapshot.publication
        )
        return FetchResult(target, snapshot.entry_count, changed=True)
    finally:
        download.unlink(missing_ok=True)


def listed_persons(path: Path, sanctions_list: SanctionsList) -> Iterator[ListedPerson]:
    """Primary names of the individuals on a landed list."""
    for _, element in iterparse(path, events=("end",)):
        tag = _local(element.tag)
        if sanctions_list is UN and tag == "INDIVIDUAL":
            parts = [_text(element, f) for f in ("FIRST_NAME", "SECOND_NAME", "THIRD_NAME", "FOURTH_NAME")]
            yield ListedPerson(UN.name, _text(element, "REFERENCE_NUMBER"), tuple(p for p in parts if p))
            element.clear()
        elif sanctions_list is OFAC and tag == "sdnEntry":
            if _text(element, "sdnType") == "Individual":
                parts = [_text(element, "firstName"), _text(element, "lastName")]
                yield ListedPerson(OFAC.name, _text(element, "uid"), tuple(p for p in parts if p))
            element.clear()
