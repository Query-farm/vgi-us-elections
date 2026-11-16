"""Unit tests. The SQL-level tests live in test/sql and run through DuckDB."""

from __future__ import annotations

import csv
import dataclasses
import io
import json
import struct
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from vgi.table_filter_pushdown import ComparisonOp, ConstantFilter, InFilter, PushdownFilters

from us_elections_worker import districts, fec, medsl, states
from us_elections_worker.common import (
    Options,
    UpstreamError,
    congress_for_election_year,
    git_blob_sha1,
    normalize_district,
    options_from,
    redact,
)
from us_elections_worker.geo import to_wkb
from us_elections_worker.pushdown import prefilter
from us_elections_worker.reference import apportionment_table

from .conftest import FIXTURES

# ---------------------------------------------------------------- join keys


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("AL", "00"),
        ("at-large", "00"),
        ("At Large", "00"),
        ("0", "00"),
        (0, "00"),
        (0.0, "00"),
        ("7", "07"),
        ("07", "07"),
        (7.0, "07"),
        ("12.0", "12"),
        ("53", "53"),
        ("98", "98"),
        ("ZZ", None),
        ("", None),
        (None, None),
        ("statewide", "00"),
    ],
)
def test_normalize_district(label: object, expected: str | None) -> None:
    assert normalize_district(label) == expected


def test_congress_for_election_year() -> None:
    assert congress_for_election_year(1788) == 1
    assert congress_for_election_year(2022) == 118
    assert congress_for_election_year(2023) == 118
    assert congress_for_election_year(2024) == 119


def test_state_lookup() -> None:
    assert states.lookup("de") == ("10", "DE", "Delaware")
    assert states.lookup("10") == ("10", "DE", "Delaware")
    assert states.lookup("6") == ("06", "CA", "California")
    assert states.lookup("6.0") == ("06", "CA", "California")
    assert states.lookup("rhode  island") == ("44", "RI", "Rhode Island")
    assert states.lookup("District Of Columbia") == ("11", "DC", "District of Columbia")
    assert states.resolve(None) is None
    assert states.resolve("  ") is None
    with pytest.raises(states.UnknownStateError, match="unknown state"):
        states.resolve("Atlantis")
    assert len(states.STATES) == 56


def test_redact_hides_api_key() -> None:
    assert redact("https://x/v1/a?state=DE&api_key=SECRET&page=2") == "https://x/v1/a?state=DE&api_key=***&page=2"


def test_options_defaults_and_overrides(tmp_path: Path) -> None:
    default = options_from({})
    assert default.dataverse_url == "https://dataverse.harvard.edu"
    assert not default.has_guestbook
    custom = options_from({"CACHE_DIR": str(tmp_path), "OFFLINE": True, "FEC_BASE_URL": "http://h/v1/", "x": None})
    assert custom.cache_dir == tmp_path and custom.offline and custom.fec_base_url == "http://h/v1"


# ---------------------------------------------------------------- geometry


def test_to_wkb_polygon_and_multipolygon() -> None:
    ring = [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]
    poly = to_wkb({"type": "Polygon", "coordinates": [ring]})
    assert poly is not None
    assert struct.unpack_from("<BIII", poly) == (1, 3, 1, 4)
    assert len(poly) == 1 + 4 + 4 + 4 + 4 * 16
    multi = to_wkb({"type": "MultiPolygon", "coordinates": [[ring], [ring]]})
    assert multi is not None
    assert struct.unpack_from("<BII", multi) == (1, 6, 2)
    assert multi[9:] == poly + poly
    assert to_wkb(None) is None
    assert to_wkb({"type": "MultiPolygon", "coordinates": []}) is None
    # Z ordinates are dropped.
    assert to_wkb({"type": "Point", "coordinates": [1, 2, 3]}) == struct.pack("<BIdd", 1, 1, 1.0, 2.0)


# ---------------------------------------------------------------- bundled reference data


def test_apportionment_table() -> None:
    table = apportionment_table()
    assert table.num_rows == 588
    rows = {(r["census_year"], r["state_po"]): r for r in table.to_pylist()}
    assert rows[(2020, "TX")]["seats"] == 38 and rows[(2020, "TX")]["seat_change"] == 2
    assert rows[(1960, "CA")]["seats"] == 38
    totals: dict[int, int] = {}
    for (year, _), r in rows.items():
        totals[year] = totals.get(year, 0) + r["seats"]
    assert totals[2020] == 435 and totals[1910] == 433
    assert all(r["state_fips"] for r in rows.values())


# ---------------------------------------------------------------- MEDSL parsing


def test_parse_house_fixture() -> None:
    table = medsl.parse("house", FIXTURES / "dataverse" / "files" / "13592823.csv")
    assert table.schema == medsl.HOUSE_SCHEMA
    rows = table.to_pylist()
    de = [r for r in rows if r["state_po"] == "DE" and r["year"] == 2024 and r["stage"] == "gen"]
    assert {r["district"] for r in de} == {"00"}
    assert de[0]["congress"] == 119 and de[0]["state"] == "Delaware" and de[0]["state_fips"] == "10"
    assert {r["stage"] for r in rows} == {"gen", "pri"}
    ny = [r for r in rows if r["state_po"] == "NY"]
    assert {r["district"] for r in ny} == {"07"}
    juliet = [r for r in ny if r["candidate"] == "CANDIDATE JULIET"]
    assert {r["party_simplified"] for r in juliet} == {"DEMOCRAT", "OTHER"}
    assert all(r["fusion_ticket"] for r in juliet)
    writein = [r for r in ny if r["writein"]]
    assert writein[0]["candidatevotes"] is None and writein[0]["vote_share"] is None
    alpha = next(r for r in rows if r["candidate"] == "CANDIDATE ALPHA")
    assert alpha["vote_share"] == pytest.approx(178416 / 323003)


def test_parse_county_fixture() -> None:
    table = medsl.parse("county_president", FIXTURES / "dataverse" / "files" / "13573089.csv")
    rows = table.to_pylist()
    autauga = [r for r in rows if r["county_name"] == "AUTAUGA"]
    assert {r["county_fips"] for r in autauga} == {"01001"}
    assert {r["state_fips"] for r in autauga} == {"01"}
    assert {r["mode"] for r in autauga} == {"election day", "absentee"}
    statewide = next(r for r in rows if r["state_po"] == "AK")
    assert statewide["county_fips"] is None and statewide["state_fips"] == "02"


def test_parse_real_senate_subset() -> None:
    table = medsl.parse("senate", FIXTURES / "dataverse" / "files" / "13887039.csv")
    assert table.num_rows == 102
    assert set(table.column("state_fips").to_pylist()) == {"04", "10", "13", "31", "56"}
    assert set(table.column("stage").to_pylist()) <= {"gen", "pre", "runoff"}
    assert table.column("candidatevotes").type == pa.int64()


def test_parse_tab_delimited(tmp_path: Path) -> None:
    src = (FIXTURES / "dataverse" / "files" / "13887042.csv").read_text()
    rows = list(csv.reader(io.StringIO(src)))
    tab = tmp_path / "president.tab"
    with tab.open("w", newline="") as handle:
        csv.writer(handle, delimiter="\t", lineterminator="\n").writerows(rows)
    expected = medsl.parse("president", FIXTURES / "dataverse" / "files" / "13887042.csv")
    assert medsl.parse("president", tab).equals(expected)


def test_signed_url_shapes() -> None:
    assert medsl._signed_url({"status": "OK", "data": {"signedUrl": "https://s/x"}}) == "https://s/x"
    assert medsl._signed_url({"data": "https://s/y"}) == "https://s/y"
    assert medsl._signed_url({"data": {"url": "http://s/z"}}) == "http://s/z"
    assert medsl._signed_url({"status": "OK"}) is None


def test_verify_rejects_wrong_file() -> None:
    with pytest.raises(UpstreamError, match="lacks expected columns"):
        medsl.verify(medsl.DATASETS["senate"], b"<html>not a csv</html>")


# ---------------------------------------------------------------- MEDSL download + cache (mock server)


def test_ensure_downloads_then_caches(opts: Options) -> None:
    first = medsl.ensure(opts, "senate")
    assert first.status == "downloaded"
    assert first.meta["file_id"] == 13887039 and first.meta["dataset_version"] == "8.0"
    assert first.meta["license"] == "CC0 1.0"
    assert medsl.ensure(opts, "senate").status == "cached"
    assert medsl.ensure(opts, "senate", refresh=True).status == "not_modified"


def test_ensure_guestbook_required(opts: Options) -> None:
    with pytest.raises(medsl.GuestbookRequiredError, match="no guestbook credentials") as err:
        medsl.ensure(opts, "house")
    assert str(medsl.data_path(opts, "house")) in str(err.value)


def test_ensure_guestbook_answered(opts: Options) -> None:
    answered = dataclasses.replace(
        opts, guestbook_name="T", guestbook_email="t@example.invalid", guestbook_institution="I", guestbook_position="P"
    )
    state = medsl.ensure(answered, "house")
    assert state.status == "downloaded"
    table, _ = medsl.load(answered, "house")
    assert table.num_rows == 16


def test_ensure_manual_file_and_offline(opts: Options) -> None:
    path = medsl.data_path(opts, "county_president")
    path.parent.mkdir(parents=True)
    path.write_bytes((FIXTURES / "dataverse" / "files" / "13573089.csv").read_bytes())
    assert medsl.ensure(opts, "county_president").status == "manual"
    offline = dataclasses.replace(opts, offline=True)
    with pytest.raises(UpstreamError, match="OFFLINE"):
        medsl.ensure(offline, "president")


# ---------------------------------------------------------------- pushdown pre-filter


def test_prefilter_narrows_on_key_columns() -> None:
    table = pa.table({"year": pa.array([2020, 2022, 2024], pa.int32()), "state_po": ["DE", "DE", "AZ"]})
    pushed = PushdownFilters(
        filters=(
            ConstantFilter(column_name="year", column_index=0, op=ComparisonOp.GE, value=pa.scalar(2022)),
            InFilter(column_name="state_po", column_index=1, values=pa.array(["DE"])),
        )
    )
    out = prefilter(table, pushed, frozenset({"year", "state_po"}))
    assert out.to_pylist() == [{"year": 2022, "state_po": "DE"}]
    assert prefilter(table, None, frozenset({"year"})).num_rows == 3
    # Columns outside the allow-list are left to the exact filter.
    assert prefilter(table, pushed, frozenset()).num_rows == 3


# ---------------------------------------------------------------- districts


def test_lewis_index_and_file_selection() -> None:
    index = districts.lewis_index()
    assert len(index) == 907
    de = districts.files_for(118, states.resolve("DE"))
    assert [f.filename for f in de] == ["Delaware_115_to_119.geojson"]
    assert len(districts.files_for(119, None)) >= 50
    data = (FIXTURES / "lewis" / "GeoJson" / de[0].filename).read_bytes()
    assert git_blob_sha1(data) == de[0].blob_sha1


def test_district_rows_from_fixture() -> None:
    f = districts.files_for(119, states.resolve("RI"))[0]
    batch = districts.district_rows((FIXTURES / "lewis" / "GeoJson" / f.filename).read_bytes(), 119, f)
    rows = batch.to_pylist()
    assert [r["district"] for r in rows] == ["01", "02"] or sorted(r["district"] for r in rows) == ["01", "02"]
    assert all(r["state_fips"] == "44" and r["geometry"][:1] == b"\x01" for r in rows)


def test_fetch_lewis_verifies_and_caches(opts: Options) -> None:
    f = districts.files_for(118, states.resolve("WY"))[0]
    data = districts.fetch_lewis(opts, f)
    assert git_blob_sha1(data) == f.blob_sha1
    assert (opts.cache_dir / "lewis").is_dir()
    bad = dataclasses.replace(f, blob_sha1="0" * 40)
    cold = dataclasses.replace(opts, cache_dir=opts.cache_dir / "other")
    with pytest.raises(UpstreamError, match="failed verification"):
        districts.fetch_lewis(cold, bad)


def test_current_rows_from_census_fixture() -> None:
    data = (FIXTURES / "census" / "cb_2024_us_cd119_20m.zip").read_bytes()
    batch = districts.current_rows(data, None)
    rows = batch.to_pylist()
    assert len(rows) == 437
    assert sum(1 for r in rows if r["district"] == "98") == 2
    wy = districts.current_rows(data, states.resolve("WY")).to_pylist()
    assert [(r["geoid"], r["district"], r["state_po"]) for r in wy] == [("5600", "00", "WY")]


# ---------------------------------------------------------------- FEC


@dataclasses.dataclass
class _Args:
    election_year: int | None = None
    state: str | None = None
    office: str | None = None
    district: str | None = None
    party: str | None = None
    name: str | None = None
    candidate_id: str | None = None


def test_candidate_query_translation() -> None:
    query = fec._candidate_query(_Args(election_year=2024, state="Delaware", district="AL", party="dem"))
    assert query["state"] == "DE" and query["district"] == "00" and query["party"] == "DEM"
    with pytest.raises(ValueError, match="at least one of"):
        fec._candidate_query(_Args(office="H"))


def test_fetch_page_and_rows(mock_api: str) -> None:
    body = fec.fetch_page(f"{mock_api}/fec/v1", "/candidates/totals/", {"election_year": 2024}, "test-key", 1)
    row = fec.FecCandidateTotals.row(body["results"][0])
    assert row["receipts"] == Decimal("9934497.35") and row["district"] == "00"
    assert row["coverage_end_date"].isoformat() == "2024-12-31"
    with pytest.raises(UpstreamError, match="rejected the API key"):
        fec.fetch_page(f"{mock_api}/fec/v1", "/candidates/", {"state": "DE"}, "nope", 1)


def test_fec_fixture_has_no_designated_agent_fields() -> None:
    body = json.loads((FIXTURES / "fec" / "committees_page1.json").read_text())
    assert not any(k.startswith("designated_agent") for r in body["results"] for k in r)
