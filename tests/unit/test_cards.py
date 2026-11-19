from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from pathlib import Path

from aml_platform.sources import cards
from aml_platform.sources.landing import sha256_of

DAY = date(2022, 9, 5)
HOLDERS = [cards.CardHolder(f"9{i:015d}", city) for i, city in enumerate(["Rabat", "Fes", "Rabat", "Agadir"] * 25)]


def _write(tmp_path: Path, *defects: cards.Defect, count: int = 2_000) -> Path:
    merchants = cards.build_merchants([h.home_city for h in HOLDERS], seed=1, per_city=50)
    return cards.generate_file(tmp_path, DAY, HOLDERS, merchants, count=count, seed=1, defects=frozenset(defects))


def test_clean_file_has_matching_trailer(tmp_path: Path) -> None:
    path = _write(tmp_path)
    parsed = cards.read_file(path)

    assert path.name == "CARDTXN_20220905_001.dat"
    assert parsed.business_date == DAY
    assert parsed.header[:4] == ("H", "CARDTXN", cards.FORMAT_VERSION, cards.PROCESSOR_CODE)
    assert len(parsed.details) == 2_000
    assert parsed.trailer_matches()
    assert all(len(record) == len(cards.DETAIL_FIELDS) for record in parsed.details)
    assert len({record[1] for record in parsed.details}) == 2_000


def test_records_are_plausible(tmp_path: Path) -> None:
    details = cards.read_file(_write(tmp_path)).details
    days = Counter(record[3][:8] for record in details)

    assert set(days) == {"20220904", "20220905"}
    assert days["20220904"] < len(details) * 0.06
    for record in details:
        assert int(record[11]) > 0
        assert (record[13] == "00") == (record[14] != "")
        if record[5] == "ATM":
            assert (record[4], record[6], record[12]) == ("WDL", "6011", cards.MAD)
            assert int(record[11]) % 10_000 == 0
        if record[10] != "MAR":
            assert record[5] == "ECOM"


def test_atm_belongs_to_a_single_city(tmp_path: Path) -> None:
    details = cards.read_file(_write(tmp_path)).details
    cities: dict[str, set[str]] = {}
    for record in details:
        if record[5] == "ATM":
            cities.setdefault(record[7], set()).add(record[9])
    assert cities
    assert all(len(seen) == 1 for seen in cities.values())


def test_generation_is_deterministic(tmp_path: Path) -> None:
    assert sha256_of(_write(tmp_path / "a")) == sha256_of(_write(tmp_path / "b"))


def test_resent_records_keep_trailer_consistent(tmp_path: Path) -> None:
    parsed = cards.read_file(_write(tmp_path, cards.Defect.DUPLICATES))
    refs = Counter(record[1] for record in parsed.details)

    assert parsed.trailer_matches()
    assert len(parsed.details) == 2_010
    assert sum(1 for n in refs.values() if n > 1) == 10


def test_truncated_file_has_no_trailer(tmp_path: Path) -> None:
    parsed = cards.read_file(_write(tmp_path, cards.Defect.TRUNCATED))

    assert len(parsed.details) < 2_000
    assert not parsed.trailer_matches()


def test_date_format_release_keeps_header_and_trailer(tmp_path: Path) -> None:
    clean = cards.read_file(_write(tmp_path / "clean"))
    released = cards.read_file(_write(tmp_path / "released", cards.Defect.DATE_FORMAT))

    assert released.header == clean.header
    assert released.trailer_matches()
    assert [d[3] for d in released.details] == [
        datetime.strptime(d[3], "%Y%m%d%H%M%S").isoformat(sep=" ") for d in clean.details
    ]
    assert [d[:3] + d[4:] for d in released.details] == [d[:3] + d[4:] for d in clean.details]


def test_business_dates_are_inclusive() -> None:
    assert list(cards.business_dates(date(2022, 9, 1), date(2022, 9, 3))) == [
        date(2022, 9, 1),
        date(2022, 9, 2),
        date(2022, 9, 3),
    ]
