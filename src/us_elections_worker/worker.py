"""Assemble the ``us_elections`` catalog and run the worker.

Run it:

    vgi-us-elections-worker                      # stdio: what DuckDB launches by default
    vgi-us-elections-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB:

    ATTACH 'us_elections' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-us-elections-worker');
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from vgi import Worker
from vgi.catalog import Catalog, Schema
from vgi.catalog.attach_option import AttachOption

from . import fec, medsl, reference
from .cache import CacheInfo
from .common import (
    DEFAULT_CENSUS_CD_BASE_URL,
    DEFAULT_DATAVERSE_URL,
    DEFAULT_FEC_BASE_URL,
    DEFAULT_LEWIS_BASE_URL,
    DEFAULT_MAX_AGE_DAYS,
)
from .districts import CurrentDistricts, Districts
from .meta import (
    CATALOG_COMMENT,
    CATALOG_NAME,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_COMMENT,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    keywords_array,
)

FUNCTIONS: list[type[Any]] = [
    Districts,
    CurrentDistricts,
    *fec.FUNCTIONS,
    *reference.SCALARS,
    CacheInfo,
]
TABLES = [*medsl.TABLES, *reference.TABLES]
SCANS: list[type[Any]] = [*medsl.SCANS, *reference.SCANS]


def _stamp_example_queries(functions: list[type[Any]]) -> list[dict[str, str]]:
    """Publish each function's examples as ``vgi.example_queries``; returns the first of each for the schema."""
    by_name: dict[str, list[dict[str, str]]] = {}
    for fn in functions:
        bucket = by_name.setdefault(fn.Meta.name, [])
        for ex in getattr(fn.Meta, "examples", []):
            entry = {"description": ex.description, "sql": ex.sql}
            if entry not in bucket:
                bucket.append(entry)
    for fn in functions:
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(by_name[fn.Meta.name])}
    first = [entries[0] for entries in by_name.values() if entries]
    for table in TABLES:
        first.append(json.loads(table.tags["vgi.example_queries"])[0])
    return first


_SCHEMA_EXAMPLES = _stamp_example_queries(FUNCTIONS)


class AttachOptions:
    """ATTACH options: cache settings, upstream mirrors and Harvard Dataverse guestbook answers."""

    cache_dir: Annotated[
        str | None,
        AttachOption(
            desc="Directory for downloaded upstream files. Default: the per-user cache directory, e.g. "
            "~/Library/Caches/vgi-us-elections or ~/.cache/vgi-us-elections."
        ),
    ] = None
    max_age_days: Annotated[
        int,
        AttachOption(desc="Re-check Harvard Dataverse for a newer MEDSL release once a cached file is this old."),
    ] = DEFAULT_MAX_AGE_DAYS
    offline: Annotated[
        bool, AttachOption(desc="Never download; use cached files only (fails for anything not cached).")
    ] = False
    dataverse_url: Annotated[
        str, AttachOption(desc="Base URL of the Dataverse installation holding the MEDSL datasets.")
    ] = DEFAULT_DATAVERSE_URL
    lewis_base_url: Annotated[
        str,
        AttachOption(desc="Base URL of the Lewis et al. boundary repository (raw files at the pinned commit)."),
    ] = DEFAULT_LEWIS_BASE_URL
    census_cd_base_url: Annotated[
        str, AttachOption(desc="Base URL of the Census cartographic boundary shapefiles (GENZ2024/shp).")
    ] = DEFAULT_CENSUS_CD_BASE_URL
    fec_base_url: Annotated[str, AttachOption(desc="Base URL of the OpenFEC API.")] = DEFAULT_FEC_BASE_URL
    guestbook_name: Annotated[
        str | None,
        AttachOption(desc="Your name, for the Harvard Dataverse guestbook that gates the House and county files."),
    ] = None
    guestbook_email: Annotated[str | None, AttachOption(desc="Your email, for the Dataverse guestbook.")] = None
    guestbook_institution: Annotated[
        str | None, AttachOption(desc="Your institution, for the Dataverse guestbook.")
    ] = None
    guestbook_position: Annotated[str | None, AttachOption(desc="Your position, for the Dataverse guestbook.")] = None
    dataverse_api_token: Annotated[
        str | None,
        AttachOption(desc="Optional Harvard Dataverse API token, sent with guestbook downloads.", secret=True),
    ] = None


class UsElectionsWorker(Worker):
    """The worker process: the ``us_elections`` catalog with one ``main`` schema."""

    AttachOptions = AttachOptions

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_COMMENT,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment=SCHEMA_COMMENT,
                functions=FUNCTIONS,
                tables=TABLES,
                tags={
                    "vgi.title": "US Elections",
                    "domain": "civic",
                    "vgi.keywords": keywords_array(
                        "election results, house, senate, president, county results, apportionment, districts, "
                        "fec, campaign finance"
                    ),
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(SCHEMA_CATEGORIES),
                    "vgi.example_queries": json.dumps(_SCHEMA_EXAMPLES),
                },
            )
        ],
    )

    secret_types = [fec.FEC_SECRET_TYPE]


_ = SCANS  # backing scans are reached through TABLES


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    UsElectionsWorker.main()


if __name__ == "__main__":
    main()
