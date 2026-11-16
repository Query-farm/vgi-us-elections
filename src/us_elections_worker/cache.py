"""``cache_info()``: what the local download cache holds, and where to put manual downloads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, ClassVar

import pyarrow as pa
from vgi.metadata import FunctionExample
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

from . import medsl
from .common import LEWIS_COMMIT, Options, options_of, read_json
from .districts import RESOLUTIONS, census_zip_name, lewis_index
from .meta import SOURCE_URL, column, object_tags, result_columns


def _fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c(
            "source",
            pa.string(),
            "'medsl', 'lewis' (historical districts) or 'census' (119th districts).",
            nullable=False,
        ),
        c(
            "item",
            pa.string(),
            "Dataset key ('house', 'senate' ...), 'geojson files', or the Census zip name.",
            nullable=False,
        ),
        c(
            "status",
            pa.string(),
            "'cached' (downloaded by the worker), 'manual' (a file you placed there) or 'missing'.",
            nullable=False,
        ),
        c("path", pa.string(), "Absolute cache path; save a manual download here.", nullable=False),
        c("files", pa.int64(), "Number of cached files (several for the per-state Lewis files)."),
        c("size_bytes", pa.int64(), "Bytes on disk; NULL when missing."),
        c("dataset_version", pa.string(), "Upstream version: Dataverse dataset version or pinned git commit."),
        c("sha256", pa.string(), "SHA-256 of the cached file (MEDSL), when recorded."),
        c("downloaded_at", pa.timestamp("us", tz="UTC"), "When the worker downloaded it."),
        c("upstream", pa.string(), "Where it comes from (DOI or URL)."),
        c("license", pa.string(), "Upstream data licence."),
    ]


CACHE_FIELDS = _fields()
CACHE_SCHEMA = pa.schema(CACHE_FIELDS)


def _ts(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value)).astimezone(UTC) if value else None
    except ValueError:
        return None


def cache_rows(opts: Options) -> dict[str, list[Any]]:
    """Describe every cached (or expected) upstream file."""
    rows: dict[str, list[Any]] = {n: [] for n in CACHE_SCHEMA.names}

    def add(**row: Any) -> None:
        for name in CACHE_SCHEMA.names:
            rows[name].append(row.get(name))

    for key, spec in medsl.DATASETS.items():
        path = medsl.data_path(opts, key)
        meta = read_json(medsl.meta_path(opts, key))
        exists = path.is_file()
        add(
            source="medsl",
            item=key,
            status=("cached" if meta else "manual") if exists else "missing",
            path=str(path.resolve()),
            files=1 if exists else 0,
            size_bytes=path.stat().st_size if exists else None,
            dataset_version=meta.get("dataset_version"),
            sha256=meta.get("sha256"),
            downloaded_at=_ts(meta.get("downloaded_at")),
            upstream=spec.doi,
            license=meta.get("license") or "CC0 1.0",
        )
    lewis_dir = opts.cache_dir / "lewis" / LEWIS_COMMIT
    known = {f.filename for f in lewis_index()}
    cached = [p for p in lewis_dir.glob("*.geojson") if p.name in known] if lewis_dir.is_dir() else []
    add(
        source="lewis",
        item="geojson files",
        status="cached" if cached else "missing",
        path=str(lewis_dir.resolve()),
        files=len(cached),
        size_bytes=sum(p.stat().st_size for p in cached) if cached else None,
        dataset_version=LEWIS_COMMIT,
        upstream="https://github.com/JeffreyBLewis/congressional-district-boundaries",
        license="MIT",
    )
    for res in RESOLUTIONS:
        path = opts.cache_dir / "census" / census_zip_name(res)
        exists = path.is_file()
        add(
            source="census",
            item=census_zip_name(res),
            status="cached" if exists else "missing",
            path=str(path.resolve()),
            files=1 if exists else 0,
            size_bytes=path.stat().st_size if exists else None,
            dataset_version="2024 cartographic boundaries (119th Congress)",
            upstream="https://www2.census.gov/geo/tiger/GENZ2024/shp/",
            license="Public domain (US government work)",
        )
    return rows


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """``cache_info()`` takes no arguments."""


@bind_fixed_schema
@init_single_worker
class CacheInfo(TableFunctionGenerator[NoArgs, None]):
    """One row per upstream file the worker caches."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = CACHE_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "cache_info"
        description = "Show which upstream election files are cached locally, their versions and paths"
        tags = {
            **object_tags(
                title="Cache Status",
                category="cache",
                keywords="cache, provenance, dataset version, download status, manual download, offline",
                doc_llm=(
                    "One row per upstream file the worker downloads: the four MEDSL result files, the Lewis "
                    "per-state district GeoJSON files (one summary row) and the three Census boundary zips. "
                    "Columns: source, item, status ('cached', 'manual', 'missing'), path (where the file lives, "
                    "and where to save a manual browser download of a guestbook-gated MEDSL file), files, "
                    "size_bytes, dataset_version, sha256, downloaded_at, upstream, license. It never downloads "
                    "anything. The cache directory is the CACHE_DIR ATTACH option or the per-user cache dir."
                ),
                doc_md=(
                    "## cache_info()\n\nWhat is cached, its upstream version and licence, and the path to put a "
                    "manually downloaded MEDSL file."
                ),
            ),
            "vgi.result_columns_schema": result_columns(CACHE_FIELDS),
            "vgi.source_url": f"{SOURCE_URL}/blob/main/src/us_elections_worker/cache.py",
        }
        examples = [
            FunctionExample(
                sql="SELECT source, item, status, files FROM us_elections.main.cache_info() ORDER BY source, item",
                description="List every cacheable upstream file and whether it is present",
            ),
        ]

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: None, out: OutputCollector) -> None:
        """Emit the cache description."""
        batch = pa.RecordBatch.from_pydict(cache_rows(options_of(params)), schema=CACHE_SCHEMA)
        out.emit(batch.select(params.output_schema.names))
        out.finish()
