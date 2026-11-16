"""Discovery metadata: catalog/schema docs, categories and the small helpers every module uses.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these tags.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "us_elections"

SOURCE_URL = "https://github.com/Query-farm/vgi-us-elections"

SCHEMA_CATEGORIES = [
    {
        "name": "results",
        "title": "Election Results",
        "description": "Certified US House, Senate and presidential returns from the MIT Election Lab.",
    },
    {
        "name": "seats",
        "title": "Seats and Apportionment",
        "description": "House seats per state after each census, from the Census Bureau.",
    },
    {
        "name": "districts",
        "title": "District Boundaries",
        "description": "Congressional district polygons, historical and current, as GEOMETRY.",
    },
    {
        "name": "finance",
        "title": "Campaign Finance",
        "description": "FEC candidates, committees, fundraising totals and contributions by state.",
    },
    {
        "name": "reference",
        "title": "Reference and Join Keys",
        "description": "State codes and district-number normalization for joining election data.",
    },
    {
        "name": "cache",
        "title": "Cache and Provenance",
        "description": "Which upstream files are cached locally, their versions and checksums.",
    },
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


def _duckdb_type(field: pa.Field[Any]) -> str:
    meta = field.metadata or {}
    if meta.get(b"ARROW:extension:name") == b"geoarrow.wkb":
        return "GEOMETRY('OGC:CRS84')"
    t = field.type
    simple = {
        pa.int16(): "SMALLINT",
        pa.int32(): "INTEGER",
        pa.int64(): "BIGINT",
        pa.float64(): "DOUBLE",
        pa.string(): "VARCHAR",
        pa.bool_(): "BOOLEAN",
        pa.date32(): "DATE",
        pa.timestamp("us", tz="UTC"): "TIMESTAMP WITH TIME ZONE",
    }
    if t in simple:
        return simple[t]
    if pa.types.is_decimal(t):
        return f"DECIMAL({t.precision},{t.scale})"
    if pa.types.is_list(t):
        return _duckdb_type(pa.field("x", t.value_type)) + "[]"
    raise ValueError(f"no DuckDB name for {t}")


def result_columns(fields: list[pa.Field[Any]] | pa.Schema) -> str:
    """Build the ``vgi.result_columns_schema`` JSON for output fields."""
    out = []
    for f in fields:
        text = (f.metadata or {}).get(b"comment", b"").decode()
        if len(text) > 300:
            text = text[:297].rsplit(" ", 1)[0] + "..."
        out.append({"name": f.name, "type": _duckdb_type(f), "description": text})
    return json.dumps(out)


def column_comments(schema: pa.Schema) -> dict[str, str]:
    """Column name -> comment, for a catalog Table's ``column_comments``."""
    return {f.name: (f.metadata or {}).get(b"comment", b"").decode() for f in schema}


CATALOG_COMMENT = (
    "US federal elections as SQL: House, Senate and presidential results since 1976 and county "
    "presidential results since 2000 (MIT Election Lab), House apportionment by census, historical "
    "and current congressional district boundaries, and FEC campaign finance."
)

CATALOG_DOC_LLM = (
    "US federal election data for DuckDB. Results come from the MIT Election Data + Science Lab "
    "(MEDSL) datasets on Harvard Dataverse (CC0): district-level US House returns 1976-2024, state-level "
    "US Senate and presidential returns 1976-2024, and county-level presidential returns 2000-2024, one "
    "row per candidate per contest with candidatevotes, totalvotes and a derived vote_share. Seats per "
    "state after each census 1910-2020 come from the Census Bureau's apportionment file (bundled). "
    "District polygons are GEOMETRY columns: every Congress from the 1st to the 119th from Lewis et al.'s "
    "UCLA boundary files, and the 119th Congress from Census cartographic boundaries. Campaign finance "
    "(candidates, committees, per-cycle totals, itemized receipts by state) comes from the FEC OpenFEC "
    "API and needs a free api.data.gov key in a `us_elections_fec` secret; everything else is keyless. "
    "Join keys: year `INTEGER`, state_po (USPS code), state_fips (two-digit `VARCHAR`), district as a "
    "two-digit `VARCHAR` where '00' is at-large, congress `INTEGER` and FEC candidate_id; member data joins "
    "to a Congress worker on (congress, state_po, district) and bioguide ids. Upstream files are "
    "downloaded on first use into a local cache and reused."
)

CATALOG_DOC_MD = (
    "# US Elections\n\n"
    "**Certified US federal election results, House apportionment, congressional district maps and FEC "
    "campaign finance as DuckDB tables.**\n\n"
    "- Results: [MIT Election Data + Science Lab](https://electionlab.mit.edu/data) on Harvard Dataverse "
    "(CC0): House, Senate and President 1976-2024, county-level President 2000-2024.\n"
    "- Apportionment: [Census Bureau](https://www.census.gov/data/tables/time-series/dec/apportionment-data-text.html) "
    "1910-2020 (public domain).\n"
    "- Districts: [Lewis et al., UCLA](https://cdmaps.polisci.ucla.edu/) (MIT licence) for the 1st-119th "
    "Congress and [Census cartographic boundaries]"
    "(https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html) "
    "for the 119th.\n"
    "- Campaign finance: the [FEC OpenFEC API](https://api.open.fec.gov/developers/) (api.data.gov key).\n\n"
    "Districts are two-digit text, `'00'` at-large; states carry both USPS and FIPS codes."
)

SCHEMA_COMMENT = (
    "Election results, apportionment, district boundaries, FEC campaign finance and join-key helpers "
    "for US federal elections."
)

SCHEMA_DOC_LLM = (
    "The `main` schema holds US federal election data. Result tables are read like ordinary tables "
    "and accept WHERE filters on year and state, which are applied before rows leave the worker. "
    "Boundary functions take a Congress number and an optional state and return polygons as GEOMETRY "
    "for spatial joins with the spatial extension. FEC functions page through the OpenFEC API and need "
    "a `us_elections_fec` secret holding an api.data.gov key. Use state_po or state_fips plus a "
    "two-digit district ('00' at-large) to join results, districts and finance; convert other spellings "
    "with the district normalizer. Vote shares are derived (candidatevotes / totalvotes); MEDSL rows "
    "include write-ins and, for some states, separate vote modes, so filter writein and mode as needed."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "US federal election results, seats, district maps and campaign finance.\n\n"
    "- **Results** are tables; filter on `year` and `state_po` and they are narrowed in the worker.\n"
    "- **Districts** are table functions returning `GEOMETRY('OGC:CRS84')`.\n"
    "- **FEC** functions need an api.data.gov key in a secret:\n\n"
    "```sql\nCREATE SECRET fec (TYPE us_elections_fec, API_KEY 'your-key');\n```"
)

AGENT_TEST_TASKS = [
    {
        "name": "delaware_2024_president",
        "prompt": "Using the us_elections worker, what share of Delaware's 2024 presidential vote did the "
        "Democratic candidate receive? Give it as a percentage rounded to one decimal place.",
    },
    {
        "name": "seats_gained_2020",
        "prompt": "Using the us_elections worker, which states gained House seats in the 2020 apportionment, "
        "and how many did each gain?",
    },
    {
        "name": "california_seats_history",
        "prompt": "How many US House seats did California have after each census from 1950 through 2020?",
    },
    {
        "name": "senate_close_races_2024",
        "prompt": "Using the us_elections worker, list the 2024 regular general-election US Senate races where "
        "the winner's margin over the runner-up was under 2 percentage points.",
    },
    {
        "name": "congress_elected_2022",
        "prompt": "Using the us_elections worker, which numbered Congress was elected in the November 2022 elections?",
    },
    {
        "name": "fips_for_nebraska",
        "prompt": "Using the us_elections worker, what is Nebraska's two-digit FIPS state code?",
    },
    {
        "name": "wyoming_districts_100th",
        "prompt": "Using the us_elections worker, how many congressional districts did Wyoming have in the 100th "
        "Congress, and what district code does it use?",
    },
    {
        "name": "largest_district_119",
        "prompt": "Using the us_elections worker, which 119th Congress district has the largest land area? Give its "
        "state code and district.",
    },
    {
        "name": "cached_sources",
        "prompt": "Using the us_elections worker, which upstream sources does the worker keep in its local download "
        "cache? List the distinct source names.",
    },
    {
        "name": "delaware_house_2024_and_counties",
        "prompt": "Using the us_elections worker, who won Delaware's at-large House seat in 2024, and in which "
        "Delaware county did the 2024 Democratic presidential candidate get the highest vote share?",
    },
    {
        "name": "delaware_senate_money_2024",
        "prompt": "Using the us_elections worker's FEC functions, who raised the most money among Delaware's 2024 "
        "US Senate candidates, how many committees are linked to them, and which state gave them the most "
        "itemized individual contributions?",
    },
    {
        "name": "normalize_at_large",
        "prompt": "Using the us_elections worker, convert the district labels 'AL', '7' and '12.0' to the "
        "worker's standard district codes.",
    },
]

CATALOG_TAGS = {
    "vgi.title": "US Federal Elections",
    "vgi.subtitle": "US federal election results, House apportionment, district maps and FEC campaign finance.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC",
    "vgi.keywords": keywords_array(
        "elections, us house, us senate, president, election results, apportionment, congressional districts, "
        "redistricting, campaign finance, fec, medsl, voting"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
