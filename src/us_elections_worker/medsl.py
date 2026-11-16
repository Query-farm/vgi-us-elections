"""MIT Election Data + Science Lab (MEDSL) results from Harvard Dataverse, as catalog tables.

Four CC0 datasets, each resolved through the Dataverse API to its latest
published file and downloaded on first use into the cache:

* U.S. House 1976-2024 (doi:10.7910/DVN/IG0UN2)
* U.S. Senate 1976-2024 (doi:10.7910/DVN/PEJ5QU)
* U.S. President 1976-2024 (doi:10.7910/DVN/42MVDX)
* County Presidential Election Returns 2000-2024 (doi:10.7910/DVN/VOQCHQ)

Harvard Dataverse puts the House and county files behind a *guestbook*: a form
(name, email, institution, position) that must be answered before the file
can be downloaded. The worker never invents an identity. Either pass your own
answers as ATTACH options (``GUESTBOOK_NAME`` ...), or download the file in a
browser and drop it into the cache directory; ``cache_info()`` shows the path.
"""

from __future__ import annotations

import io
import json
import logging
import re
import threading
import time
import urllib.parse
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

import pyarrow as pa
import pyarrow.compute as _pc
import pyarrow.csv as pacsv
from vgi.catalog import Table
from vgi.catalog.constraints import constraint_record
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
    HttpStatusError,
    Options,
    UpstreamError,
    get_json,
    options_of,
    read_json,
    request,
    sha256_hex,
    write_atomic,
    write_json,
)
from .meta import column, column_comments, object_tags, result_columns
from .pushdown import prefilter

log = logging.getLogger(__name__)
pc: Any = _pc

BATCH_ROWS = 16384
PREFILTER_COLUMNS = frozenset({"year", "state_po", "state_fips", "district", "congress", "county_fips", "stage"})


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    """One MEDSL dataset on Harvard Dataverse."""

    key: str
    doi: str
    file_pattern: str
    title: str
    required_columns: frozenset[str]

    @property
    def landing_url(self) -> str:
        """The dataset's Dataverse landing page."""
        return f"https://dataverse.harvard.edu/dataset.xhtml?persistentId={self.doi}"


DATASETS: dict[str, DatasetSpec] = {
    "house": DatasetSpec(
        "house",
        "doi:10.7910/DVN/IG0UN2",
        r"^\d{4}-\d{4}-house\.(tab|csv)$",
        "U.S. House 1976-2024",
        frozenset({"year", "state_po", "district", "candidate", "party", "candidatevotes", "totalvotes"}),
    ),
    "senate": DatasetSpec(
        "senate",
        "doi:10.7910/DVN/PEJ5QU",
        r"^\d{4}-\d{4}-senate.*\.(tab|csv)$",
        "U.S. Senate 1976-2024",
        frozenset({"year", "state_po", "candidate", "party_detailed", "candidatevotes", "totalvotes"}),
    ),
    "president": DatasetSpec(
        "president",
        "doi:10.7910/DVN/42MVDX",
        r"^\d{4}-\d{4}-president\.(tab|csv)$",
        "U.S. President 1976-2024",
        frozenset({"year", "state_po", "candidate", "party_detailed", "candidatevotes", "totalvotes"}),
    ),
    "county_president": DatasetSpec(
        "county_president",
        "doi:10.7910/DVN/VOQCHQ",
        r"^countypres_\d{4}-\d{4}\.(tab|csv)$",
        "County Presidential Election Returns 2000-2024",
        frozenset({"year", "state_po", "county_fips", "candidate", "party", "candidatevotes", "totalvotes"}),
    ),
}

_lock = threading.Lock()
_memo: dict[tuple[str, Path], tuple[float, int, pa.Table]] = {}


class GuestbookRequiredError(UpstreamError):
    """Dataverse wants a guestbook response and no guestbook credentials were given."""


# ---------------------------------------------------------------- Dataverse resolution + cache


def _dir(opts: Options) -> Path:
    return opts.cache_dir / "medsl"


def data_path(opts: Options, key: str) -> Path:
    """Where the dataset's CSV lives in the cache (drop a manual download here)."""
    return _dir(opts) / f"{key}.csv"


def meta_path(opts: Options, key: str) -> Path:
    """Return the provenance sidecar for a cached dataset."""
    return _dir(opts) / f"{key}.meta.json"


def resolve_file(opts: Options, spec: DatasetSpec) -> dict[str, Any]:
    """Ask Dataverse for the dataset's latest version and pick the data file.

    Args:
        opts: Options (the Dataverse base URL).
        spec: The dataset.

    Returns:
        ``file_id``, ``filename``, ``dataset_version``, ``original_size``, ``license``, ``guestbook_id``.

    Raises:
        UpstreamError: The dataset or its data file could not be found.

    """
    query = urllib.parse.urlencode({"persistentId": spec.doi})
    body = get_json(f"{opts.dataverse_url}/api/datasets/:persistentId/?{query}")
    latest = (body.get("data") or {}).get("latestVersion") or {}
    for entry in latest.get("files") or []:
        df = entry.get("dataFile") or {}
        name = str(df.get("filename") or entry.get("label") or "")
        if re.match(spec.file_pattern, name):
            lic = latest.get("license")
            return {
                "file_id": int(df["id"]),
                "filename": name,
                "dataset_version": f"{latest.get('versionNumber')}.{latest.get('versionMinorNumber', 0)}",
                "original_size": df.get("originalFileSize") or df.get("filesize"),
                "license": lic.get("name") if isinstance(lic, dict) else lic,
                "guestbook_id": latest.get("guestbookId"),
                "release_time": latest.get("releaseTime"),
            }
    raise UpstreamError(f"Dataverse dataset {spec.doi} has no file matching {spec.file_pattern}")


def _signed_url(payload: Any) -> str | None:
    """Find the signed download URL in a guestbook POST response, whatever its key is called."""
    if isinstance(payload, str) and payload.startswith(("http://", "https://")):
        return payload
    if isinstance(payload, dict):
        for key in ("signedUrl", "signedURL", "url", "data"):
            found = _signed_url(payload.get(key))
            if found:
                return found
    return None


def _guestbook_message(opts: Options, spec: DatasetSpec) -> str:
    return (
        f"Harvard Dataverse requires a guestbook response before {spec.title} ({spec.doi}) can be downloaded, "
        "and no guestbook credentials were provided. Either ATTACH with your own answers "
        "(GUESTBOOK_NAME, GUESTBOOK_EMAIL, GUESTBOOK_INSTITUTION, GUESTBOOK_POSITION, and DATAVERSE_API_TOKEN if "
        f"your account needs one), or download the data file from {spec.landing_url} in a browser and save it "
        f"as {data_path(opts, spec.key)}"
    )


def download(opts: Options, spec: DatasetSpec, info: dict[str, Any]) -> bytes:
    """Download the data file in its original (CSV) format, answering the guestbook when configured.

    Args:
        opts: Options (URLs, guestbook answers, token).
        spec: The dataset.
        info: The resolved file (see `resolve_file`).

    Returns:
        The file bytes.

    Raises:
        GuestbookRequiredError: Dataverse wants a guestbook response and none was configured.
        HttpStatusError: Dataverse refused the download for another reason.
        UpstreamError: The download failed.

    """
    url = f"{opts.dataverse_url}/api/access/datafile/{info['file_id']}?format=original"
    headers = {"X-Dataverse-key": opts.dataverse_api_token} if opts.dataverse_api_token else {}
    try:
        return request(url, headers=headers, accept="text/csv, */*")
    except HttpStatusError as err:
        if b"guestbook" not in err.body.lower():
            raise
    if not opts.has_guestbook:
        raise GuestbookRequiredError(_guestbook_message(opts, spec))
    answer = {
        "guestbookResponse": {
            "name": opts.guestbook_name,
            "email": opts.guestbook_email,
            "institution": opts.guestbook_institution,
            "position": opts.guestbook_position,
        }
    }
    reply = request(
        url,
        data=json.dumps(answer).encode(),
        headers={**headers, "Content-Type": "application/json"},
    )
    try:
        signed = _signed_url(json.loads(reply))
    except ValueError:
        signed = None
    if not signed:
        raise UpstreamError(f"Dataverse accepted the guestbook response but returned no download URL: {reply[:200]!r}")
    return request(signed, accept="text/csv, */*")


def verify(spec: DatasetSpec, data: bytes) -> None:
    """Check that a download is the expected MEDSL table (header columns present).

    Args:
        spec: The dataset.
        data: The file bytes.

    Raises:
        UpstreamError: The payload is not the expected table.

    """
    first = data[:4096].split(b"\n", 1)[0].decode("utf-8-sig", errors="replace")
    header = {h.strip().strip('"').lower() for h in re.split(r"[\t,]", first)}
    missing = spec.required_columns - header
    if missing:
        raise UpstreamError(f"{spec.title} download lacks expected columns {sorted(missing)}: {first[:200]!r}")


@dataclass(frozen=True, slots=True)
class CacheState:
    """What the cache holds for one dataset after `ensure`."""

    path: Path
    meta: dict[str, Any]
    status: str


def ensure(opts: Options, key: str, *, refresh: bool = False) -> CacheState:
    """Make sure a dataset is cached, downloading it or re-checking its version when due.

    Args:
        opts: Options and cache settings.
        key: Dataset key ('house', 'senate', 'president', 'county_president').
        refresh: Re-check Dataverse now regardless of age.

    Returns:
        The path, provenance and what happened: 'cached', 'downloaded', 'not_modified',
        'stale' (a re-check failed), 'offline' or 'manual' (a file you placed in the cache).

    Raises:
        UpstreamError: No usable copy exists and it cannot be downloaded.

    """
    spec = DATASETS[key]
    path, mpath = data_path(opts, key), meta_path(opts, key)
    with _lock:
        meta = read_json(mpath)
        exists = path.is_file()
        if exists and not meta:
            return CacheState(path, {"source": "manual"}, "manual")
        if exists and meta.get("dataverse_url") != opts.dataverse_url:
            exists, meta = False, {}  # filled from another server (a mirror or fixtures); do not mix
        if exists and not refresh:
            checked = float(meta.get("checked_epoch", path.stat().st_mtime))
            if time.time() - checked < opts.max_age_days * 86400:
                return CacheState(path, meta, "cached")
        if opts.offline:
            if exists:
                return CacheState(path, meta, "offline")
            raise UpstreamError(f"OFFLINE is set and {spec.title} is not cached at {path}")
        try:
            info = resolve_file(opts, spec)
        except UpstreamError as err:
            if exists:
                log.warning("Dataverse re-check failed, serving the cached %s: %s", key, err)
                return CacheState(path, meta, "stale")
            raise
        now = datetime.now(UTC).isoformat()
        if exists and meta.get("file_id") == info["file_id"]:
            meta = {**meta, "checked_at": now, "checked_epoch": time.time()}
            write_json(mpath, meta)
            return CacheState(path, meta, "not_modified")
        data = download(opts, spec, info)
        verify(spec, data)
        write_atomic(path, data)
        meta = {
            **info,
            "doi": spec.doi,
            "dataverse_url": opts.dataverse_url,
            "downloaded_at": now,
            "checked_at": now,
            "checked_epoch": time.time(),
            "sha256": sha256_hex(data),
            "size_bytes": len(data),
        }
        write_json(mpath, meta)
        log.info("downloaded %s (%d bytes) to %s", spec.title, len(data), path)
        return CacheState(path, meta, "downloaded")


# ---------------------------------------------------------------- parsing


def _read_csv(path: Path) -> pa.Table:
    data = path.read_bytes()
    first = data[:4096].split(b"\n", 1)[0]
    delimiter = "\t" if first.count(b"\t") > first.count(b",") else ","
    header = [h.strip().strip('"').lower() for h in first.decode("utf-8-sig").strip().split(delimiter)]
    return pacsv.read_csv(
        io.BytesIO(data),
        read_options=pacsv.ReadOptions(column_names=header, skip_rows=1),
        parse_options=pacsv.ParseOptions(delimiter=delimiter),
        convert_options=pacsv.ConvertOptions(
            column_types=dict.fromkeys(header, pa.string()),
            null_values=["", "NA", "N/A", "NULL"],
            strings_can_be_null=True,
        ),
    )


def _col(raw: pa.Table, name: str) -> Any:
    if name in raw.column_names:
        return pc.utf8_trim_whitespace(raw.column(name))
    return pa.nulls(raw.num_rows, pa.string())


def _number(raw: pa.Table, name: str, type_: pa.DataType) -> Any:
    text = _col(raw, name)
    ok = pc.match_substring_regex(text, r"^-?\d+(\.\d*)?([eE][-+]?\d+)?$")
    values = pc.cast(pc.if_else(ok, text, pa.scalar(None, pa.string())), pa.float64())
    if pa.types.is_integer(type_):
        values = pc.round(values)
    return pc.cast(values, type_)


def _bool(raw: pa.Table, name: str) -> Any:
    text = pc.utf8_upper(_col(raw, name))
    true = pc.is_in(text, value_set=pa.array(["TRUE", "T", "1", "YES"]))
    false = pc.is_in(text, value_set=pa.array(["FALSE", "F", "0", "NO"]))
    return pc.if_else(true, True, pc.if_else(false, False, pa.scalar(None, pa.bool_())))


def _lower(raw: pa.Table, name: str) -> Any:
    return pc.utf8_lower(_col(raw, name))


def _fips(raw: pa.Table, name: str, width: int) -> Any:
    """Zero-pad a numeric FIPS code ('4' -> '04', '1001.0' -> '01001'); junk becomes NULL."""
    numbers = _number(raw, name, pa.int64())
    text = pc.cast(numbers, pa.string())
    return pc.utf8_lpad(text, width=width, padding="0")


def _state_columns(raw: pa.Table) -> tuple[Any, Any, Any]:
    """state_po, state_fips and the proper-case state name, from MEDSL's columns."""
    po = pc.utf8_upper(_col(raw, "state_po"))
    fips = _fips(raw, "state_fips", 2) if "state_fips" in raw.column_names else None
    names = [states.BY_USPS.get(p, (None, None, None))[2] for p in po.to_pylist()]
    by_po = [states.BY_USPS.get(p, (None, None, None))[0] for p in po.to_pylist()]
    from_po = pa.array(by_po, pa.string())
    fips = from_po if fips is None else pc.coalesce(fips, from_po)
    fallback = pc.utf8_title(_col(raw, "state"))
    return po, fips, pc.coalesce(pa.array(names, pa.string()), fallback)


def _stage(raw: pa.Table) -> Any:
    stage = _lower(raw, "stage")
    return pc.replace_substring_regex(stage, r"^gen runoff$", "runoff")


def _simplify_party(party: Any) -> Any:
    """MEDSL-style party_simplified for the House file, which has no such column (derived)."""
    upper = pc.fill_null(pc.utf8_upper(party), "")
    dem = pc.or_(pc.match_substring(upper, "DEMOCRAT"), pc.equal(upper, "DFL"))
    rep = pc.or_(pc.match_substring(upper, "REPUBLICAN"), pc.equal(upper, "GOP"))
    lib = pc.or_(pc.match_substring(upper, "LIBERTARIAN"), pc.equal(upper, "LBN"))
    return pc.if_else(dem, "DEMOCRAT", pc.if_else(rep, "REPUBLICAN", pc.if_else(lib, "LIBERTARIAN", "OTHER")))


def _share(votes: Any, total: Any) -> Any:
    total_f = pc.cast(total, pa.float64())
    safe = pc.if_else(pc.greater(total_f, 0), total_f, pa.scalar(None, pa.float64()))
    return pc.divide(pc.cast(votes, pa.float64()), safe)


def _district(raw: pa.Table) -> Any:
    text = pc.utf8_upper(_col(raw, "district"))
    numbers = _number(raw, "district", pa.int64())
    padded = pc.utf8_lpad(pc.cast(numbers, pa.string()), width=2, padding="0")
    at_large = pc.is_in(text, value_set=pa.array(["AT-LARGE", "AT LARGE", "AL", "STATEWIDE"]))
    return pc.if_else(at_large, "00", padded)


def _congress(year: Any) -> Any:
    return pc.cast(pc.floor(pc.divide(pc.cast(pc.subtract(year, 1786), pa.float64()), 2.0)), pa.int32())


# Column docs shared by the four tables.
C_YEAR = "Election year (INTEGER). Join key with the other tables and with FEC cycles."
C_STATE = "State name, proper case ('Delaware'), derived from state_po."
C_STATE_PO = "Two-letter USPS state code ('DE'); the same code FEC uses."
C_STATE_FIPS = "Two-digit FIPS state code as VARCHAR with the leading zero ('06')."
C_CANDIDATE = (
    "Candidate name as MEDSL publishes it (upper case; 'LAST, FIRST' in older years). NULL for some write-ins."
)
C_PARTY = "Party label exactly as on the ballot or in the official return ('DEMOCRATIC-FARMER-LABOR')."
C_PARTY_SIMPLE = "Simplified party: DEMOCRAT, REPUBLICAN, LIBERTARIAN or OTHER."
C_WRITEIN = "TRUE for write-in candidates (including aggregated 'scattering' rows)."
C_VOTES = "Votes for this candidate in this contest (and mode)."
C_TOTAL = "Total votes cast in the contest (all candidates); the denominator of vote_share."
C_SHARE = "Derived fraction (0 to 1) of the contest's votes: candidatevotes / totalvotes; NULL when totalvotes is 0."
C_MODE = "Vote mode, lower case: 'total' for most rows; some states report election-day/absentee/early separately."
C_VERSION = "MEDSL's version stamp for the row's source vintage (e.g. '20250203')."
C_STAGE = "Election stage, lower case: 'gen' (general), 'pre' (primary) or 'runoff'."
C_SPECIAL = "TRUE for a special election (filling a vacancy)."
C_UNOFFICIAL = "TRUE when MEDSL marked the count unofficial (not yet certified when compiled)."
C_CONGRESS = (
    "Derived: the Congress elected at a regular general election in this year, (year - 1786) / 2 "
    "(2024 -> 119). Special elections in even years fill a seat in the sitting Congress (one less)."
)


def _house_fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c("year", pa.int32(), C_YEAR, nullable=False),
        c("congress", pa.int32(), C_CONGRESS, nullable=False),
        c("state", pa.string(), C_STATE),
        c("state_po", pa.string(), C_STATE_PO, nullable=False),
        c("state_fips", pa.string(), C_STATE_FIPS),
        c(
            "district",
            pa.string(),
            "House district as two-digit VARCHAR: '01', '02' ... and '00' for an at-large (single-seat) state. "
            "Joins to districts() and current_districts() and to FEC's district.",
        ),
        c("stage", pa.string(), C_STAGE),
        c("runoff", pa.bool_(), "TRUE for a runoff contest."),
        c("special", pa.bool_(), C_SPECIAL),
        c("candidate", pa.string(), C_CANDIDATE),
        c("party_detailed", pa.string(), C_PARTY),
        c("party_simplified", pa.string(), "Derived from party_detailed: DEMOCRAT, REPUBLICAN, LIBERTARIAN or OTHER."),
        c("writein", pa.bool_(), C_WRITEIN),
        c("mode", pa.string(), C_MODE),
        c("candidatevotes", pa.int64(), C_VOTES),
        c("totalvotes", pa.int64(), C_TOTAL),
        c("vote_share", pa.float64(), C_SHARE),
        c("unofficial", pa.bool_(), C_UNOFFICIAL),
        c(
            "fusion_ticket",
            pa.bool_(),
            "TRUE when the candidate ran on several party lines (New York, Connecticut ...): one row per line; "
            "sum the rows for the candidate's total.",
        ),
        c("version", pa.string(), C_VERSION),
    ]


def _senate_fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c("year", pa.int32(), C_YEAR, nullable=False),
        c("congress", pa.int32(), C_CONGRESS, nullable=False),
        c("state", pa.string(), C_STATE),
        c("state_po", pa.string(), C_STATE_PO, nullable=False),
        c("state_fips", pa.string(), C_STATE_FIPS),
        c("stage", pa.string(), C_STAGE),
        c("special", pa.bool_(), C_SPECIAL),
        c("candidate", pa.string(), C_CANDIDATE),
        c("party_detailed", pa.string(), C_PARTY),
        c("party_simplified", pa.string(), C_PARTY_SIMPLE),
        c("writein", pa.bool_(), C_WRITEIN),
        c("mode", pa.string(), C_MODE),
        c("candidatevotes", pa.int64(), C_VOTES),
        c("totalvotes", pa.int64(), C_TOTAL),
        c("vote_share", pa.float64(), C_SHARE),
        c("unofficial", pa.bool_(), C_UNOFFICIAL),
        c("version", pa.string(), C_VERSION),
    ]


def _president_fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c("year", pa.int32(), C_YEAR, nullable=False),
        c("state", pa.string(), C_STATE),
        c("state_po", pa.string(), C_STATE_PO, nullable=False),
        c("state_fips", pa.string(), C_STATE_FIPS),
        c("candidate", pa.string(), C_CANDIDATE),
        c("party_detailed", pa.string(), C_PARTY),
        c("party_simplified", pa.string(), C_PARTY_SIMPLE),
        c("writein", pa.bool_(), C_WRITEIN),
        c("candidatevotes", pa.int64(), C_VOTES),
        c("totalvotes", pa.int64(), C_TOTAL),
        c("vote_share", pa.float64(), C_SHARE),
        c("notes", pa.string(), "MEDSL's free-text note on the row, usually NULL."),
        c("version", pa.string(), C_VERSION),
    ]


def _county_fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c("year", pa.int32(), C_YEAR, nullable=False),
        c("state", pa.string(), C_STATE),
        c("state_po", pa.string(), C_STATE_PO, nullable=False),
        c("state_fips", pa.string(), "Two-digit FIPS state code ('06'), from county_fips or state_po."),
        c("county_name", pa.string(), "County (or county-equivalent) name, upper case as MEDSL publishes it."),
        c(
            "county_fips",
            pa.string(),
            "Five-digit county FIPS code as VARCHAR ('01001'); NULL for non-county reporting units "
            "(overseas, federal precincts, statewide write-ins).",
        ),
        c("candidate", pa.string(), C_CANDIDATE),
        c("party", pa.string(), "MEDSL's party grouping: DEMOCRAT, REPUBLICAN, LIBERTARIAN, GREEN or OTHER."),
        c("mode", pa.string(), C_MODE + " Do not add 'total' to the other modes."),
        c("candidatevotes", pa.int64(), C_VOTES),
        c("totalvotes", pa.int64(), C_TOTAL),
        c("vote_share", pa.float64(), C_SHARE),
        c("version", pa.string(), C_VERSION),
    ]


HOUSE_SCHEMA = pa.schema(_house_fields())
SENATE_SCHEMA = pa.schema(_senate_fields())
PRESIDENT_SCHEMA = pa.schema(_president_fields())
COUNTY_SCHEMA = pa.schema(_county_fields())


def _finish(schema: pa.Schema, columns: dict[str, Any]) -> pa.Table:
    """Assemble the typed table, dropping rows without a year or state (blank trailer lines)."""
    keep = pc.and_(pc.is_valid(columns["year"]), pc.is_valid(columns["state_po"]))

    def as_array(value: Any, type_: pa.DataType) -> Any:
        value = pc.cast(value, type_)
        return value.combine_chunks() if isinstance(value, pa.ChunkedArray) else value

    arrays = [pc.filter(as_array(columns[f.name], f.type), keep) for f in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def parse(key: str, path: Path) -> pa.Table:
    """Parse a cached MEDSL file into its typed table."""
    raw = _read_csv(path)
    year = _number(raw, "year", pa.int32())
    po, fips, name = _state_columns(raw)
    votes = _number(raw, "candidatevotes", pa.int64())
    total = _number(raw, "totalvotes", pa.int64())
    common = {
        "year": year,
        "state": name,
        "state_po": po,
        "state_fips": fips,
        "candidate": _col(raw, "candidate"),
        "candidatevotes": votes,
        "totalvotes": total,
        "vote_share": _share(votes, total),
        "version": _col(raw, "version"),
    }
    if key == "house":
        party = _col(raw, "party")
        cols = {
            **common,
            "congress": _congress(year),
            "district": _district(raw),
            "stage": _stage(raw),
            "runoff": _bool(raw, "runoff"),
            "special": _bool(raw, "special"),
            "party_detailed": party,
            "party_simplified": _simplify_party(party),
            "writein": _bool(raw, "writein"),
            "mode": _lower(raw, "mode"),
            "unofficial": _bool(raw, "unofficial"),
            "fusion_ticket": _bool(raw, "fusion_ticket"),
        }
        return _finish(HOUSE_SCHEMA, cols)
    if key == "senate":
        cols = {
            **common,
            "congress": _congress(year),
            "stage": _stage(raw),
            "special": _bool(raw, "special"),
            "party_detailed": _col(raw, "party_detailed"),
            "party_simplified": pc.utf8_upper(_col(raw, "party_simplified")),
            "writein": _bool(raw, "writein"),
            "mode": _lower(raw, "mode"),
            "unofficial": _bool(raw, "unofficial"),
        }
        return _finish(SENATE_SCHEMA, cols)
    if key == "president":
        cols = {
            **common,
            "party_detailed": _col(raw, "party_detailed"),
            "party_simplified": pc.utf8_upper(_col(raw, "party_simplified")),
            "writein": _bool(raw, "writein"),
            "notes": _col(raw, "notes"),
        }
        return _finish(PRESIDENT_SCHEMA, cols)
    county = _fips(raw, "county_fips", 5)
    cols = {
        **common,
        "state_fips": pc.coalesce(pc.utf8_slice_codeunits(county, 0, 2), fips),
        "county_name": _col(raw, "county_name"),
        "county_fips": county,
        "party": pc.utf8_upper(_col(raw, "party")),
        "mode": _lower(raw, "mode"),
    }
    return _finish(COUNTY_SCHEMA, cols)


def load(opts: Options, key: str) -> tuple[pa.Table, CacheState]:
    """Return the parsed table for a dataset, downloading and parsing it once per process."""
    state = ensure(opts, key)
    stat = state.path.stat()
    with _lock:
        hit = _memo.get((key, state.path))
        if hit and hit[0] == stat.st_mtime and hit[1] == stat.st_size:
            return hit[2], state
    table = parse(key, state.path)
    with _lock:
        _memo[(key, state.path)] = (stat.st_mtime, stat.st_size, table)
    return table, state


# ---------------------------------------------------------------- scans and tables


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """The result tables take no arguments; filter them with WHERE."""


@dataclass(kw_only=True)
class ScanCursor(ArrowSerializableDataclass):
    """Row offset into the full parsed table and the file it was taken from."""

    offset: int = 0
    sha256: str = ""


class _ResultsScan(TableFunctionGenerator[NoArgs, ScanCursor]):
    """Shared scan: slice the cached table, pre-filter on key columns, emit projected batches."""

    DATASET: ClassVar[str] = ""

    @classmethod
    def initial_state(cls, params: ProcessParams[NoArgs]) -> ScanCursor:
        """Make sure the dataset is cached and remember which file this scan reads."""
        _, state = load(options_of(params), cls.DATASET)
        return ScanCursor(sha256=str(state.meta.get("sha256", "")))

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: ScanCursor, out: OutputCollector) -> None:
        """Emit the next non-empty slice of rows that can match the pushed-down filters."""
        table, cache = load(options_of(params), cls.DATASET)
        if state.sha256 and cache.meta.get("sha256") and cache.meta["sha256"] != state.sha256:
            raise UpstreamError(f"the cached {cls.DATASET} file was refreshed during this scan; run the query again")
        names = params.output_schema.names
        while state.offset < table.num_rows:
            chunk = table.slice(state.offset, BATCH_ROWS)
            state.offset += chunk.num_rows
            chunk = prefilter(chunk, params.current_pushdown_filters, PREFILTER_COLUMNS)
            if chunk.num_rows:
                out.emit(
                    pa.RecordBatch.from_arrays(
                        [chunk.column(n).combine_chunks() for n in names], schema=params.output_schema
                    )
                )
                return
        out.finish()


def _scan_meta(name: str, title: str) -> dict[str, Any]:
    return object_tags(
        title=f"{title} (scan)",
        category="results",
        keywords=f"{name}, scan",
        doc_llm=f"Backing scan of the {name} table; query the table instead.",
        doc_md=f"Backing scan of `{name}`.",
    )


@bind_fixed_schema
@init_single_worker
class HouseResultsScan(_ResultsScan):
    """Rows of ``house_results``."""

    DATASET: ClassVar[str] = "house"
    FIXED_SCHEMA: ClassVar[pa.Schema] = HOUSE_SCHEMA

    class Meta:
        """Scan metadata: projection and filter pushdown."""

        name = "house_results_scan"
        description = "Backing scan of the house_results table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = _scan_meta("house_results", "US House Results")


@bind_fixed_schema
@init_single_worker
class SenateResultsScan(_ResultsScan):
    """Rows of ``senate_results``."""

    DATASET: ClassVar[str] = "senate"
    FIXED_SCHEMA: ClassVar[pa.Schema] = SENATE_SCHEMA

    class Meta:
        """Scan metadata: projection and filter pushdown."""

        name = "senate_results_scan"
        description = "Backing scan of the senate_results table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = _scan_meta("senate_results", "US Senate Results")


@bind_fixed_schema
@init_single_worker
class PresidentResultsScan(_ResultsScan):
    """Rows of ``president_results``."""

    DATASET: ClassVar[str] = "president"
    FIXED_SCHEMA: ClassVar[pa.Schema] = PRESIDENT_SCHEMA

    class Meta:
        """Scan metadata: projection and filter pushdown."""

        name = "president_results_scan"
        description = "Backing scan of the president_results table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = _scan_meta("president_results", "US President Results")


@bind_fixed_schema
@init_single_worker
class CountyPresidentResultsScan(_ResultsScan):
    """Rows of ``county_president_results``."""

    DATASET: ClassVar[str] = "county_president"
    FIXED_SCHEMA: ClassVar[pa.Schema] = COUNTY_SCHEMA

    class Meta:
        """Scan metadata: projection and filter pushdown."""

        name = "county_president_results_scan"
        description = "Backing scan of the county_president_results table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = _scan_meta("county_president_results", "County President Results")


def _not_null(table: str, cols: list[str]) -> tuple[Any, ...]:
    return tuple(
        constraint_record(
            ["main"], table, f"{c}_not_null", "NOT_NULL", [[c]], enforcement="ENFORCED", validation="ALL_ROWS"
        )
        for c in cols
    )


_PUSHDOWN_NOTE = (
    "WHERE filters on year, state_po, state_fips{extra} are applied inside the worker before rows are sent; "
    "other filters are applied exactly too, just after a full read of the cached file."
)

_GUESTBOOK_NOTE = (
    " Harvard Dataverse requires a guestbook response (name, email, institution, position) to download this "
    "file: ATTACH with GUESTBOOK_NAME, GUESTBOOK_EMAIL, GUESTBOOK_INSTITUTION and GUESTBOOK_POSITION, or save "
    "a browser download into the cache (see cache_info())."
)


def _table_tags(
    *,
    name: str,
    title: str,
    keywords: str,
    doc_llm: str,
    doc_md: str,
    schema: pa.Schema,
    spec: DatasetSpec,
    examples: list[dict[str, str]],
) -> dict[str, str]:
    return {
        **object_tags(title=title, category="results", keywords=keywords, doc_llm=doc_llm, doc_md=doc_md),
        "vgi.result_columns_schema": result_columns(schema),
        "vgi.source_url": spec.landing_url,
        "domain": "civic",
        "vgi.example_queries": json.dumps(examples),
    }


HOUSE_TABLE = Table(
    name="house_results",
    function=HouseResultsScan,
    comment="US House returns by district, 1976-2024: one row per candidate (and party line) per contest (MEDSL, CC0).",
    column_comments=column_comments(HOUSE_SCHEMA),
    constraints=_not_null("house_results", ["year", "congress", "state_po"]),
    tags=_table_tags(
        name="house_results",
        title="US House Election Results",
        keywords="us house, congressional elections, house races, district results, midterms, representatives, "
        "election returns, medsl",
        doc_llm=(
            "District-level US House election returns 1976-2024 from the MIT Election Data + Science Lab (CC0). "
            "One row per candidate per party line per contest: year, congress (derived), state, state_po, "
            "state_fips, district ('00' at-large), stage ('gen', 'pre', 'runoff'), runoff, special, candidate, "
            "party_detailed, party_simplified (derived), writein, mode, candidatevotes, totalvotes, vote_share "
            "(derived), unofficial, fusion_ticket, version. Fusion candidates (NY, CT) have a row per party "
            "line: GROUP BY candidate and sum candidatevotes for their total. Filter stage = 'gen' for general "
            "elections. " + _PUSHDOWN_NOTE.format(extra=", district and congress") + _GUESTBOOK_NOTE
        ),
        doc_md=(
            "## house_results\n\nUS House returns by district, 1976-2024, from the "
            "[MIT Election Lab](https://doi.org/10.7910/DVN/IG0UN2) (CC0).\n\n"
            "- `district` is two-digit text; `'00'` means at-large.\n"
            "- Fusion candidates have one row per party line.\n"
            "- The Dataverse file needs a guestbook response to download; see the README."
        ),
        schema=HOUSE_SCHEMA,
        spec=DATASETS["house"],
        examples=[
            {
                "description": "Winners of Delaware's at-large House general elections since 2016",
                "sql": "SELECT year, candidate, party_simplified, round(100 * vote_share, 1) AS pct "
                "FROM us_elections.main.house_results WHERE state_po = 'DE' AND stage = 'gen' AND year >= 2016 "
                "QUALIFY row_number() OVER (PARTITION BY year ORDER BY candidatevotes DESC) = 1 ORDER BY year",
            },
            {
                "description": "Two-party House popular vote by year",
                "sql": "SELECT year, sum(candidatevotes) FILTER (WHERE party_simplified = 'DEMOCRAT') AS dem, "
                "sum(candidatevotes) FILTER (WHERE party_simplified = 'REPUBLICAN') AS rep "
                "FROM us_elections.main.house_results WHERE stage = 'gen' AND year >= 2016 GROUP BY year ORDER BY year",
            },
        ],
    ),
)

SENATE_TABLE = Table(
    name="senate_results",
    function=SenateResultsScan,
    comment="US Senate returns by state, 1976-2024: one row per candidate per contest (MEDSL, CC0).",
    column_comments=column_comments(SENATE_SCHEMA),
    constraints=_not_null("senate_results", ["year", "congress", "state_po"]),
    tags=_table_tags(
        name="senate_results",
        title="US Senate Election Results",
        keywords="us senate, senate races, senators, statewide elections, election returns, medsl",
        doc_llm=(
            "State-level US Senate election returns 1976-2024 from the MIT Election Data + Science Lab (CC0). One "
            "row per candidate per contest: year, congress (derived), state, state_po, state_fips, stage "
            "('gen', 'pre', 'runoff'), special, candidate, party_detailed, party_simplified, writein, mode, "
            "candidatevotes, totalvotes, vote_share (derived), unofficial, version. A state can hold a regular "
            "and a special Senate election in the same year: partition by (year, state_po, special). "
            + _PUSHDOWN_NOTE.format(extra=" and congress")
        ),
        doc_md=(
            "## senate_results\n\nUS Senate returns by state, 1976-2024, from the "
            "[MIT Election Lab](https://doi.org/10.7910/DVN/PEJ5QU) (CC0). Regular and special contests in the "
            "same year are separate (`special`)."
        ),
        schema=SENATE_SCHEMA,
        spec=DATASETS["senate"],
        examples=[
            {
                "description": "Winner and vote share of every 2024 regular general Senate race",
                "sql": "SELECT state_po, candidate, party_simplified, round(100 * vote_share, 1) AS pct "
                "FROM us_elections.main.senate_results WHERE year = 2024 AND stage = 'gen' AND NOT special "
                "QUALIFY row_number() OVER (PARTITION BY state_po ORDER BY candidatevotes DESC) = 1 ORDER BY state_po",
            },
            {
                "description": "Delaware Senate general-election results in 2024",
                "sql": "SELECT candidate, party_detailed, candidatevotes, round(100 * vote_share, 2) AS pct "
                "FROM us_elections.main.senate_results WHERE year = 2024 AND state_po = 'DE' AND stage = 'gen' "
                "ORDER BY candidatevotes DESC",
            },
        ],
    ),
)

PRESIDENT_TABLE = Table(
    name="president_results",
    function=PresidentResultsScan,
    comment="US presidential returns by state, 1976-2024: one row per candidate per state (MEDSL, CC0).",
    column_comments=column_comments(PRESIDENT_SCHEMA),
    constraints=_not_null("president_results", ["year", "state_po"]),
    tags=_table_tags(
        name="president_results",
        title="Presidential Returns by State",
        keywords="president, presidential election, popular vote, electoral, statewide results, medsl",
        doc_llm=(
            "State-level US presidential general-election returns 1976-2024 from the MIT Election Data + Science "
            "Lab (CC0). One row per candidate per state: year, state, state_po, state_fips, candidate, "
            "party_detailed, party_simplified, writein, candidatevotes, totalvotes, vote_share (derived), notes, "
            "version. Includes DC. National popular vote = sum over states. " + _PUSHDOWN_NOTE.format(extra="")
        ),
        doc_md=(
            "## president_results\n\nUS presidential returns by state, 1976-2024, from the "
            "[MIT Election Lab](https://doi.org/10.7910/DVN/42MVDX) (CC0)."
        ),
        schema=PRESIDENT_SCHEMA,
        spec=DATASETS["president"],
        examples=[
            {
                "description": "National popular vote by party in 2020 and 2024",
                "sql": "SELECT year, party_simplified, sum(candidatevotes) AS votes "
                "FROM us_elections.main.president_results WHERE year IN (2020, 2024) "
                "GROUP BY ALL ORDER BY year, votes DESC",
            },
            {
                "description": "Delaware's 2024 presidential result",
                "sql": "SELECT candidate, party_simplified, candidatevotes, round(100 * vote_share, 1) AS pct "
                "FROM us_elections.main.president_results WHERE year = 2024 AND state_po = 'DE' AND NOT writein "
                "ORDER BY candidatevotes DESC",
            },
        ],
    ),
)

COUNTY_TABLE = Table(
    name="county_president_results",
    function=CountyPresidentResultsScan,
    comment="US presidential returns by county, 2000-2024, with five-digit county FIPS codes (MEDSL, CC0).",
    column_comments=column_comments(COUNTY_SCHEMA),
    constraints=_not_null("county_president_results", ["year", "state_po"]),
    tags=_table_tags(
        name="county_president_results",
        title="County Presidential Returns",
        keywords="county results, presidential election, county fips, swing counties, election map, medsl",
        doc_llm=(
            "County-level US presidential returns 2000-2024 from the MIT Election Data + Science Lab (CC0). One "
            "row per candidate per county per vote mode: year, state, state_po, state_fips, county_name, "
            "county_fips (five-digit `VARCHAR`, joins to Census county data), candidate, party (DEMOCRAT, "
            "REPUBLICAN, LIBERTARIAN, GREEN, OTHER), mode, candidatevotes, totalvotes, vote_share (derived), "
            "version. Most counties report mode = 'total'; some states (e.g. 2020 GA, NC) report only per-mode "
            "rows, so sum over modes when no 'total' row exists and never add 'total' to the others. "
            + _PUSHDOWN_NOTE.format(extra=" and county_fips")
            + _GUESTBOOK_NOTE
        ),
        doc_md=(
            "## county_president_results\n\nUS presidential returns by county, 2000-2024, from the "
            "[MIT Election Lab](https://doi.org/10.7910/DVN/VOQCHQ) (CC0). Check `mode` before summing.\n\n"
            "The Dataverse file needs a guestbook response to download; see the README."
        ),
        schema=COUNTY_SCHEMA,
        spec=DATASETS["county_president"],
        examples=[
            {
                "description": "Delaware's three counties in 2024: Democratic share",
                "sql": "SELECT county_name, county_fips, round(100 * sum(candidatevotes) FILTER "
                "(WHERE party = 'DEMOCRAT') / sum(candidatevotes), 1) AS dem_pct "
                "FROM us_elections.main.county_president_results WHERE year = 2024 AND state_po = 'DE' "
                "GROUP BY ALL ORDER BY county_fips",
            },
        ],
    ),
)

TABLES = [HOUSE_TABLE, SENATE_TABLE, PRESIDENT_TABLE, COUNTY_TABLE]
SCANS: list[type[Any]] = [HouseResultsScan, SenateResultsScan, PresidentResultsScan, CountyPresidentResultsScan]
