"""Congressional district boundaries as GEOMETRY.

* ``districts(congress, state := ...)``: every district in force in a given
  Congress (1st-119th), from Jeffrey B. Lewis, Brandon DeVine, Lincoln Pritcher
  and Kenneth C. Martis, *Digital Boundary Definitions of United States
  Congressional Districts, 1789-2012* and its updates (MIT licence), read from
  the project's per-state GeoJSON files pinned to one git commit. Each file is
  downloaded on first use, verified against its git blob hash and cached.
* ``current_districts(state := ..., resolution := ...)``: the 119th Congress
  from the Census Bureau's 2024 cartographic boundary shapefile (public domain).
"""

from __future__ import annotations

import io
import json
import logging
import threading
import zipfile
from dataclasses import dataclass
from functools import cache
from importlib import resources
from pathlib import Path
from typing import Annotated, Any, ClassVar

import pyarrow as pa
import shapefile
from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from . import states
from .common import (
    LEWIS_COMMIT,
    Options,
    UpstreamError,
    git_blob_sha1,
    normalize_district,
    options_of,
    quote_path,
    request,
    write_atomic,
)
from .geo import geometry_field, to_wkb
from .meta import column, object_tags, result_columns

log = logging.getLogger(__name__)
_lock = threading.Lock()

MAX_CONGRESS = 119
LEWIS_SOURCE = "https://cdmaps.polisci.ucla.edu/"
CENSUS_SOURCE = "https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html"
RESOLUTIONS = ("500k", "5m", "20m")


@dataclass(frozen=True, slots=True)
class LewisFile:
    """One per-state GeoJSON file of the Lewis et al. boundaries."""

    state_name: str
    start_congress: int
    end_congress: int
    blob_sha1: str
    size_bytes: int

    @property
    def filename(self) -> str:
        """File name inside the repository's GeoJson/ folder."""
        return f"{self.state_name}_{self.start_congress:03d}_to_{self.end_congress:03d}.geojson"


@cache
def lewis_index() -> tuple[LewisFile, ...]:
    """Return the bundled listing of the pinned commit's GeoJSON files."""
    raw = json.loads(resources.files(__package__).joinpath("data/lewis_index.json").read_text())
    return tuple(LewisFile(*row) for row in raw["files"])


def _state_of_lewis(name: str) -> tuple[str, str, str] | None:
    return states.lookup("District of Columbia" if name.lower() == "district of columbia" else name)


def files_for(congress: int, state: tuple[str, str, str] | None) -> list[LewisFile]:
    """Return the files that may hold districts in force in ``congress`` (optionally one state)."""
    out = []
    for f in lewis_index():
        if not f.start_congress <= congress <= f.end_congress:
            continue
        found = _state_of_lewis(f.state_name)
        if state is not None and (found is None or found[0] != state[0]):
            continue
        out.append(f)
    return sorted(out, key=lambda f: (f.state_name, f.start_congress, f.end_congress))


def fetch_lewis(opts: Options, f: LewisFile) -> bytes:
    """Return one GeoJSON file from the cache, downloading and verifying it on first use.

    Args:
        opts: Options (base URL, cache dir, offline).
        f: The file.

    Returns:
        The file bytes.

    Raises:
        UpstreamError: Not cached and cannot be downloaded, or the download fails verification.

    """
    path = opts.cache_dir / "lewis" / LEWIS_COMMIT / f.filename
    if path.is_file():
        return path.read_bytes()
    if opts.offline:
        raise UpstreamError(f"OFFLINE is set and {f.filename} is not cached at {path}")
    data = request(f"{opts.lewis_base_url}/GeoJson/{quote_path(f.filename)}", accept="application/geo+json, */*")
    if git_blob_sha1(data) != f.blob_sha1:
        raise UpstreamError(
            f"{f.filename} failed verification (git blob {git_blob_sha1(data)} != expected {f.blob_sha1}); "
            "the mirror may serve a different revision"
        )
    with _lock:
        write_atomic(path, data)
    return data


def _int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


# ---------------------------------------------------------------- districts(congress, state := ...)


def _district_fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c("congress", pa.int32(), "The Congress requested (1 = 1789-1791 ... 119 = 2025-2027).", nullable=False),
        c("state", pa.string(), "State name, proper case ('Delaware').", nullable=False),
        c("state_po", pa.string(), "Two-letter USPS state code ('DE')."),
        c("state_fips", pa.string(), "Two-digit FIPS state code as VARCHAR ('10')."),
        c(
            "district",
            pa.string(),
            "District as two-digit VARCHAR ('01'); '00' is an at-large seat (a single-seat state, or a seat "
            "elected statewide alongside districts).",
        ),
        c("start_congress", pa.int32(), "First Congress these boundaries were used for."),
        c("end_congress", pa.int32(), "Last Congress these boundaries were used for (inclusive)."),
        c("district_id", pa.string(), "Lewis et al. identifier: state FIPS (3), start, end and district (3 each)."),
        c("law", pa.string(), "Statute or court order that drew the district, when the source records it."),
        c("note", pa.string(), "Source notes on how the boundary was reconstructed."),
        geometry_field("District boundary as GEOMETRY('OGC:CRS84') polygons (lon/lat, NAD83 passed through)."),
    ]


DISTRICT_FIELDS = _district_fields()
DISTRICT_SCHEMA = pa.schema(DISTRICT_FIELDS)


def district_rows(data: bytes, congress: int, f: LewisFile) -> pa.RecordBatch:
    """Parse one Lewis GeoJSON file into rows for the districts in force in ``congress``."""
    doc = json.loads(data)
    found = _state_of_lewis(f.state_name)
    rows: dict[str, list[Any]] = {name: [] for name in DISTRICT_SCHEMA.names}
    for feature in doc.get("features") or []:
        props = feature.get("properties") or {}
        start = _int(props.get("startcong")) or f.start_congress
        end = _int(props.get("endcong")) or f.end_congress
        if not start <= congress <= end:
            continue
        rows["congress"].append(congress)
        rows["state"].append(found[2] if found else f.state_name)
        rows["state_po"].append(found[1] if found else None)
        rows["state_fips"].append(found[0] if found else _text(props.get("statefp")))
        rows["district"].append(normalize_district(props.get("district")))
        rows["start_congress"].append(start)
        rows["end_congress"].append(end)
        rows["district_id"].append(_text(props.get("id")))
        rows["law"].append(_text(props.get("law")))
        rows["note"].append(_text(props.get("note")))
        rows["geometry"].append(to_wkb(feature.get("geometry")))
    return pa.RecordBatch.from_pydict(rows, schema=DISTRICT_SCHEMA)


@dataclass(slots=True, frozen=True, kw_only=True)
class DistrictsArgs:
    """Arguments for ``districts(congress, state := NULL)``."""

    congress: Annotated[int, Arg(0, doc="Congress number, 1 (1789) to 119 (2025-2027).", ge=1, le=MAX_CONGRESS)]
    state: Annotated[
        str | None,
        Arg("state", default=None, doc="Limit to one state: USPS code ('DE'), FIPS code ('10') or name."),
    ]


@dataclass(kw_only=True)
class FileCursor(ArrowSerializableDataclass):
    """Index of the next file to read."""

    index: int = 0


_DISTRICTS_DOC_LLM = (
    "Boundaries of every US congressional district in force in one Congress, 1st (1789) through 119th "
    "(2025-2027), from Lewis, DeVine, Pritcher and Martis's UCLA district boundary files (MIT licence). "
    "Positional argument: congress (`INTEGER` 1-119). Named: state := 'DE' (USPS code, FIPS code or name); "
    "strongly recommended, because each state is a separate download of up to 14 MB the first time. Columns: "
    "congress, state, state_po, state_fips, district (two-digit `VARCHAR`, '00' at-large), start_congress, "
    "end_congress, district_id, law, note, geometry (GEOMETRY('OGC:CRS84') polygons). States that elected "
    "all members at large in a Congress return a single '00' row or, where no map exists, no rows. Join to "
    "house_results on (congress, state_po, district) and to Congress member data the same way. Use the "
    "spatial extension for ST_Area, ST_Contains and friends."
)


@bind_fixed_schema
@init_single_worker
class Districts(TableFunctionGenerator[DistrictsArgs, FileCursor]):
    """Historical congressional district polygons for one Congress."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DISTRICT_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "districts"
        description = "Congressional district boundaries for any Congress from 1789 to today, as GEOMETRY"
        tags = {
            **object_tags(
                title="Historical Congressional Districts",
                category="districts",
                keywords="congressional districts, district boundaries, redistricting, gerrymandering, district map, "
                "shapefile, geojson, historical maps, ucla cdmaps",
                doc_llm=_DISTRICTS_DOC_LLM,
                doc_md=(
                    "## districts(congress, state := NULL)\n\n"
                    "District polygons for any Congress, 1st-119th, from "
                    "[Lewis et al., UCLA](https://cdmaps.polisci.ucla.edu/) (MIT licence).\n\n"
                    "- Pass `state :=`; each state's file is downloaded once and cached.\n"
                    "- `district` is two-digit text, `'00'` at-large."
                ),
            ),
            "vgi.result_columns_schema": result_columns(DISTRICT_FIELDS),
            "vgi.source_url": LEWIS_SOURCE,
        }
        examples = [
            FunctionExample(
                sql="SELECT congress, state_po, district, start_congress, end_congress "
                "FROM us_elections.main.districts(118, state := 'DE') ORDER BY district",
                description="Delaware's at-large district in the 118th Congress",
            ),
            FunctionExample(
                sql="SELECT congress, count(*) AS districts FROM ("
                "SELECT * FROM us_elections.main.districts(100, state := 'WY') UNION ALL "
                "SELECT * FROM us_elections.main.districts(119, state := 'WY')) GROUP BY congress ORDER BY congress",
                description="Wyoming's district count in the 100th and 119th Congress",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[DistrictsArgs]) -> FileCursor:
        """Validate the state argument before any download."""
        states.resolve(params.args.state)
        return FileCursor()

    @classmethod
    def process(cls, params: ProcessParams[DistrictsArgs], state: FileCursor, out: OutputCollector) -> None:
        """Read the next state file and emit its districts in force in the requested Congress."""
        files = files_for(params.args.congress, states.resolve(params.args.state))
        opts = options_of(params)
        while state.index < len(files):
            f = files[state.index]
            state.index += 1
            batch = district_rows(fetch_lewis(opts, f), params.args.congress, f)
            if batch.num_rows:
                out.emit(batch.select(params.output_schema.names))
                return
        out.finish()


# ---------------------------------------------------------------- current_districts(...)


def _current_fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c("congress", pa.int32(), "Always 119: the Congress these boundaries are drawn for.", nullable=False),
        c("state", pa.string(), "State or territory name ('Delaware')."),
        c("state_po", pa.string(), "Two-letter USPS code ('DE')."),
        c("state_fips", pa.string(), "Two-digit FIPS state code ('10').", nullable=False),
        c(
            "district",
            pa.string(),
            "Two-digit district ('01'); '00' at-large; '98' for the non-voting delegate seats (DC, PR, territories).",
        ),
        c("geoid", pa.string(), "Census GEOID: state FIPS + district ('1000').", nullable=False),
        c("name", pa.string(), "Census name ('Congressional District (at Large)')."),
        c("land_area_m2", pa.int64(), "Land area in square metres (Census ALAND)."),
        c("water_area_m2", pa.int64(), "Water area in square metres (Census AWATER)."),
        geometry_field("District boundary as GEOMETRY('OGC:CRS84') (lon/lat; NAD83 passed through)."),
    ]


CURRENT_FIELDS = _current_fields()
CURRENT_SCHEMA = pa.schema(CURRENT_FIELDS)


def census_zip_name(resolution: str) -> str:
    """Return the Census cartographic boundary file name for a resolution ('500k', '5m', '20m')."""
    return f"cb_2024_us_cd119_{resolution}.zip"


def fetch_census(opts: Options, resolution: str) -> bytes:
    """Return the Census CD119 shapefile zip from the cache, downloading it on first use.

    Args:
        opts: Options (base URL, cache dir, offline).
        resolution: '500k', '5m' or '20m'.

    Returns:
        The zip bytes.

    Raises:
        UpstreamError: Not cached and cannot be downloaded, or the download is not the shapefile.

    """
    name = census_zip_name(resolution)
    path = opts.cache_dir / "census" / name
    if path.is_file():
        return path.read_bytes()
    if opts.offline:
        raise UpstreamError(f"OFFLINE is set and {name} is not cached at {path}")
    data = request(f"{opts.census_cd_base_url}/{name}", accept="application/zip, */*")
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            members = {Path(n).suffix for n in archive.namelist()}
    except zipfile.BadZipFile as err:
        raise UpstreamError(f"{name} download is not a zip file: {err}") from err
    if not {".shp", ".shx", ".dbf"} <= members:
        raise UpstreamError(f"{name} lacks shapefile members (has {sorted(members)})")
    with _lock:
        write_atomic(path, data)
    return data


def current_rows(data: bytes, state: tuple[str, str, str] | None) -> pa.RecordBatch:
    """Parse the Census CD119 shapefile zip into rows, optionally for one state."""
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        names = archive.namelist()

        def member(suffix: str) -> io.BytesIO:
            return io.BytesIO(archive.read(next(n for n in names if n.endswith(suffix))))

        reader = shapefile.Reader(shp=member(".shp"), shx=member(".shx"), dbf=member(".dbf"))
        rows: dict[str, list[Any]] = {name: [] for name in CURRENT_SCHEMA.names}
        for sr in reader.iterShapeRecords():
            if sr.record is None:
                continue
            rec = sr.record.as_dict()
            fips = str(rec.get("STATEFP") or "").zfill(2)
            if state is not None and fips != state[0]:
                continue
            found = states.BY_FIPS.get(fips)
            code = str(rec.get("CD119FP") or "")
            rows["congress"].append(119)
            rows["state"].append(found[2] if found else None)
            rows["state_po"].append(found[1] if found else None)
            rows["state_fips"].append(fips)
            rows["district"].append("98" if code == "98" else normalize_district(code))
            rows["geoid"].append(str(rec.get("GEOID") or f"{fips}{code}"))
            rows["name"].append(_text(rec.get("NAMELSAD")))
            rows["land_area_m2"].append(_int(rec.get("ALAND")))
            rows["water_area_m2"].append(_int(rec.get("AWATER")))
            shape = sr.shape
            geojson: Any = shape.__geo_interface__ if shape is not None and shape.points else None
            rows["geometry"].append(to_wkb(geojson))
    order = sorted(range(len(rows["geoid"])), key=lambda i: rows["geoid"][i])
    return pa.RecordBatch.from_pydict({k: [v[i] for i in order] for k, v in rows.items()}, schema=CURRENT_SCHEMA)


@dataclass(slots=True, frozen=True, kw_only=True)
class CurrentArgs:
    """Arguments for ``current_districts(state := NULL, resolution := '500k')``."""

    state: Annotated[
        str | None,
        Arg("state", default=None, doc="Limit to one state or territory: USPS code ('DE'), FIPS code or name."),
    ]
    resolution: Annotated[
        str,
        Arg(
            "resolution",
            default="500k",
            doc="Census generalisation: '500k' (most detail, ~7 MB download), '5m' or '20m' (~0.4 MB).",
            choices=list(RESOLUTIONS),
        ),
    ]


@dataclass(kw_only=True)
class DoneCursor(ArrowSerializableDataclass):
    """Whether the single batch was sent."""

    done: bool = False


@bind_fixed_schema
@init_single_worker
class CurrentDistricts(TableFunctionGenerator[CurrentArgs, DoneCursor]):
    """119th Congress district polygons from the Census Bureau."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = CURRENT_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "current_districts"
        description = "119th Congress district boundaries from the Census Bureau, with land and water area"
        tags = {
            **object_tags(
                title="Current Congressional Districts (119th)",
                category="districts",
                keywords="119th congress, congressional districts, census cartographic boundary, district map, "
                "district area, geoid, current districts",
                doc_llm=(
                    "Polygons of the 119th Congress's districts (2025-2027) from the Census Bureau's 2024 "
                    "cartographic boundary file (public domain): all 435 voting districts plus the non-voting "
                    "delegate areas (district '98'). Named: state := 'DE' (USPS, FIPS or name), resolution := "
                    "'500k' | '5m' | '20m' (generalisation; '20m' is a small download for maps). Columns: "
                    "congress (119), state, state_po, state_fips, district ('00' at-large), geoid, name, "
                    "land_area_m2, water_area_m2, geometry (GEOMETRY('OGC:CRS84')). The file is downloaded "
                    "once and cached. Join to house_results (year 2024, congress 119) on state_po + district."
                ),
                doc_md=(
                    "## current_districts(state := NULL, resolution := '500k')\n\n"
                    "The 119th Congress's districts from the Census "
                    "[cartographic boundary files](" + CENSUS_SOURCE + ") (public domain), with land/water area."
                ),
            ),
            "vgi.result_columns_schema": result_columns(CURRENT_FIELDS),
            "vgi.source_url": CENSUS_SOURCE,
        }
        examples = [
            FunctionExample(
                sql="SELECT state_po, count(*) AS districts, round(sum(land_area_m2) / 1e6) AS land_km2 "
                "FROM us_elections.main.current_districts(resolution := '20m') WHERE district <> '98' "
                "GROUP BY state_po ORDER BY districts DESC, state_po LIMIT 5",
                description="States with the most districts in the 119th Congress, with their land area",
            ),
            FunctionExample(
                sql="SELECT geoid, district, name FROM us_elections.main.current_districts("
                "state := 'WY', resolution := '20m') ORDER BY geoid",
                description="Wyoming's at-large district",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[CurrentArgs]) -> DoneCursor:
        """Validate the state argument before any download."""
        states.resolve(params.args.state)
        return DoneCursor()

    @classmethod
    def process(cls, params: ProcessParams[CurrentArgs], state: DoneCursor, out: OutputCollector) -> None:
        """Emit every matching district in one batch."""
        if state.done:
            out.finish()
            return
        state.done = True
        data = fetch_census(options_of(params), params.args.resolution)
        batch = current_rows(data, states.resolve(params.args.state))
        out.emit(batch.select(params.output_schema.names))
