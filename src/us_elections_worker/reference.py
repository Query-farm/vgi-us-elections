"""Bundled reference data and join-key helpers.

* ``apportionment``: House seats per state after each census 1910-2020, from the
  Census Bureau's historical apportionment file (public domain, bundled).
* ``states``: FIPS / USPS / name for the states, DC and territories.
* ``normalize_district(text)`` and ``congress_for_year(year)``: scalars that put
  other sources' district labels and years onto this worker's join keys.
"""

from __future__ import annotations

import io
import json
from dataclasses import dataclass
from functools import cache
from importlib import resources
from typing import Annotated, Any, ClassVar

import pyarrow as pa
import pyarrow.compute as _pc
import pyarrow.csv as pacsv
from vgi.arguments import Param, Returns
from vgi.catalog import Table
from vgi.catalog.constraints import constraint_record
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

from . import states
from .common import normalize_district
from .meta import column, column_comments, object_tags, result_columns

pc: Any = _pc

APPORTIONMENT_SOURCE = "https://www.census.gov/data/tables/time-series/dec/apportionment-data-text.html"
APPORTIONMENT_FILE = "https://www2.census.gov/programs-surveys/decennial/2020/data/apportionment/apportionment.csv"


def _apportionment_fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c("census_year", pa.int32(), "Decennial census year, 1910 to 2020.", nullable=False),
        c("state", pa.string(), "State name ('California').", nullable=False),
        c("state_po", pa.string(), "Two-letter USPS state code ('CA').", nullable=False),
        c("state_fips", pa.string(), "Two-digit FIPS state code ('06').", nullable=False),
        c(
            "seats",
            pa.int32(),
            "US House seats apportioned to the state from this census. 1920: Congress did not reapportion, "
            "so the Census lists the seats in force (from 1910).",
            nullable=False,
        ),
        c("seat_change", pa.int32(), "Change in seats from the previous census; NULL for the first year listed."),
        c("resident_population", pa.int64(), "Number of residents counted by the census (persons)."),
        c("population_change_pct", pa.float64(), "Percent change in resident population since the previous census."),
        c("population_density", pa.float64(), "Residents per square mile of land."),
        c(
            "avg_population_per_seat",
            pa.int64(),
            "Average apportionment population per representative (the Census's district-size figure).",
        ),
    ]


APPORTIONMENT_SCHEMA = pa.schema(_apportionment_fields())


def _num(table: pa.Table, name: str, type_: pa.DataType) -> Any:
    text = pc.replace_substring(pc.utf8_trim_whitespace(table.column(name)), ",", "")
    ok = pc.match_substring_regex(text, r"^-?\d+(\.\d*)?$")
    values = pc.cast(pc.if_else(ok, text, pa.scalar(None, pa.string())), pa.float64())
    if pa.types.is_integer(type_):
        values = pc.round(values)
    return pc.cast(values, type_)


@cache
def apportionment_table() -> pa.Table:
    """Parse the bundled Census apportionment CSV: states only, rows with seats."""
    data = resources.files(__package__).joinpath("data/apportionment.csv").read_bytes()
    raw = pacsv.read_csv(
        io.BytesIO(data),
        convert_options=pacsv.ConvertOptions(strings_can_be_null=True, column_types={}, null_values=[""]),
        read_options=pacsv.ReadOptions(),
    )
    raw = raw.cast(pa.schema([pa.field(n, pa.string()) for n in raw.column_names]))
    raw = raw.filter(pc.equal(raw.column("Geography Type"), "State"))
    seats = _num(raw, "Number of Representatives", pa.int32())
    raw, seats = raw.filter(pc.is_valid(seats)), seats.filter(pc.is_valid(seats))
    found = [states.lookup(n) for n in raw.column("Name").to_pylist()]
    order = pa.array(range(raw.num_rows))
    table = pa.table(
        {
            "census_year": _num(raw, "Year", pa.int32()),
            "state": pa.array([f[2] if f else None for f in found], pa.string()),
            "state_po": pa.array([f[1] if f else None for f in found], pa.string()),
            "state_fips": pa.array([f[0] if f else None for f in found], pa.string()),
            "seats": seats,
            "seat_change": _num(raw, "Change in Number of Representatives", pa.int32()),
            "resident_population": _num(raw, "Resident Population", pa.int64()),
            "population_change_pct": _num(raw, "Percent Change in Resident Population", pa.float64()),
            "population_density": _num(raw, "Resident Population Density", pa.float64()),
            "avg_population_per_seat": _num(raw, "Average Apportionment Population Per Representative", pa.int64()),
            "_order": order,
        }
    )
    table = table.sort_by([("census_year", "ascending"), ("state_po", "ascending")]).drop_columns(["_order"])
    return pa.Table.from_arrays(
        [table.column(n).combine_chunks() for n in APPORTIONMENT_SCHEMA.names], schema=APPORTIONMENT_SCHEMA
    )


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """Reference tables take no arguments."""


@bind_fixed_schema
@init_single_worker
class ApportionmentScan(TableFunctionGenerator[NoArgs, None]):
    """Rows of ``apportionment``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = APPORTIONMENT_SCHEMA

    class Meta:
        """Scan metadata."""

        name = "apportionment_scan"
        description = "Backing scan of the apportionment table"
        projection_pushdown = True
        tags = object_tags(
            title="Apportionment (scan)",
            category="seats",
            keywords="apportionment, scan",
            doc_llm="Backing scan of the apportionment table; query the table instead.",
            doc_md="Backing scan of `apportionment`.",
        )

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: None, out: OutputCollector) -> None:
        """Emit every (census year, state) row."""
        table = apportionment_table().select(params.output_schema.names)
        out.emit(
            pa.RecordBatch.from_arrays(
                [table.column(n).combine_chunks() for n in table.column_names], schema=params.output_schema
            )
        )
        out.finish()


def _key(table: str, cols: list[str]) -> tuple[Any, ...]:
    records = [
        constraint_record(
            ["main"], table, f"{c}_not_null", "NOT_NULL", [[c]], enforcement="ENFORCED", validation="ALL_ROWS"
        )
        for c in cols
    ]
    return tuple(records)


def _pk(table: str, cols: list[str]) -> Any:
    return constraint_record(
        ["main"], table, "pk", "PRIMARY_KEY", [[c] for c in cols], enforcement="ENFORCED", validation="ALL_ROWS"
    )


_APPORTIONMENT_EXAMPLES: list[dict[str, Any]] = [
    {
        "description": "States that gained House seats in the 2020 apportionment",
        "sql": "SELECT state_po, seats, seat_change FROM us_elections.main.apportionment "
        "WHERE census_year = 2020 AND seat_change > 0 ORDER BY seat_change DESC, state_po",
        "expected_result": [["TX", 38, 2], ["CO", 8, 1], ["FL", 28, 1], ["MT", 2, 1], ["NC", 14, 1], ["OR", 6, 1]],
    },
    {
        "description": "California's House seats after each census since 1950",
        "sql": "SELECT census_year, seats FROM us_elections.main.apportionment "
        "WHERE state_po = 'CA' AND census_year >= 1950 ORDER BY census_year",
        "expected_result": [
            [1950, 30],
            [1960, 38],
            [1970, 43],
            [1980, 45],
            [1990, 52],
            [2000, 53],
            [2010, 53],
            [2020, 52],
        ],
    },
    {
        "description": "Total House seats: 433 after 1910 (Arizona and New Mexico added 2 on admission), then 435",
        "sql": "SELECT census_year, sum(seats) AS house_seats FROM us_elections.main.apportionment "
        "WHERE census_year IN (1910, 1960, 2020) GROUP BY census_year ORDER BY census_year",
        "expected_result": [[1910, 433], [1960, 435], [2020, 435]],
    },
]

APPORTIONMENT_TABLE = Table(
    name="apportionment",
    function=ApportionmentScan,
    comment="US House seats, population and seats change per state after each census, 1910-2020 (Census Bureau).",
    column_comments=column_comments(APPORTIONMENT_SCHEMA),
    constraints=(
        *_key("apportionment", ["census_year", "state", "state_po", "state_fips", "seats"]),
        _pk("apportionment", ["census_year", "state_po"]),
    ),
    tags={
        **object_tags(
            title="House Apportionment by Census",
            category="seats",
            keywords="apportionment, house seats, reapportionment, census, seats per state, population, "
            "representatives, electoral votes",
            doc_llm=(
                "US House seats per state after each decennial census 1910-2020, from the Census Bureau's "
                "historical apportionment file (public domain, bundled, no download). One row per (census_year, "
                "state) for states that had seats: census_year, state, state_po, state_fips, seats, seat_change "
                "(vs the previous census), resident_population, population_change_pct, population_density, "
                "avg_population_per_seat. Seats from census year Y apply to Congresses elected from Y+2 (2020 "
                "census -> 2022 elections -> 118th Congress). Electoral votes = seats + 2. 1920 had no "
                "reapportionment. Earlier censuses (1790-1900) are only published as PDF and are not included."
            ),
            doc_md=(
                "## apportionment\n\nHouse seats per state after each census, 1910-2020, from the "
                "[Census Bureau](" + APPORTIONMENT_SOURCE + ") (public domain)."
            ),
        ),
        "vgi.result_columns_schema": result_columns(APPORTIONMENT_SCHEMA),
        "vgi.source_url": APPORTIONMENT_SOURCE,
        "domain": "civic",
        "vgi.executable_examples": json.dumps(_APPORTIONMENT_EXAMPLES),
        "vgi.example_queries": json.dumps(
            [{"description": e["description"], "sql": e["sql"]} for e in _APPORTIONMENT_EXAMPLES]
        ),
    },
)


# ---------------------------------------------------------------- states


def _states_fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c("state_fips", pa.string(), "Two-digit FIPS state code ('06').", nullable=False),
        c("state_po", pa.string(), "Two-letter USPS code ('CA').", nullable=False),
        c("state", pa.string(), "Name ('California').", nullable=False),
        c("kind", pa.string(), "'state', 'district' (DC) or 'territory'.", nullable=False),
    ]


STATES_SCHEMA = pa.schema(_states_fields())


@bind_fixed_schema
@init_single_worker
class StatesScan(TableFunctionGenerator[NoArgs, None]):
    """Rows of ``states``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = STATES_SCHEMA

    class Meta:
        """Scan metadata."""

        name = "states_scan"
        description = "Backing scan of the states table"
        tags = object_tags(
            title="States (scan)",
            category="reference",
            keywords="states, scan",
            doc_llm="Backing scan of the states table; query the table instead.",
            doc_md="Backing scan of `states`.",
        )

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: None, out: OutputCollector) -> None:
        """Emit the 56 states, DC and territories."""
        cols = list(zip(*states.STATES, strict=True))
        batch = pa.RecordBatch.from_arrays([pa.array(list(v), pa.string()) for v in cols], schema=STATES_SCHEMA)
        out.emit(batch.select(params.output_schema.names))
        out.finish()


_STATES_EXAMPLES: list[dict[str, Any]] = [
    {
        "description": "Look up the FIPS code of a USPS abbreviation",
        "sql": "SELECT state_fips, state FROM us_elections.main.states WHERE state_po = 'DE'",
        "expected_result": [["10", "Delaware"]],
    },
    {
        "description": "Count states, DC and territories",
        "sql": "SELECT kind, count(*) AS n FROM us_elections.main.states GROUP BY kind ORDER BY kind",
        "expected_result": [["district", 1], ["state", 50], ["territory", 5]],
    },
]

STATES_TABLE = Table(
    name="states",
    function=StatesScan,
    comment="US states, DC and territories with FIPS and USPS codes: the join keys every table here uses.",
    column_comments=column_comments(STATES_SCHEMA),
    constraints=(*_key("states", ["state_fips", "state_po", "state", "kind"]), _pk("states", ["state_fips"])),
    tags={
        **object_tags(
            title="States and Territories",
            category="reference",
            keywords="state codes, fips, usps abbreviation, postal code, state names, territories, crosswalk",
            doc_llm=(
                "One row per US state, DC and the five inhabited territories: state_fips (two-digit `VARCHAR` "
                "'06'), state_po (USPS 'CA'), state (name) and kind ('state', 'district', 'territory'). Use it "
                "to translate between the FIPS codes Census data uses and the USPS codes FEC and MEDSL use."
            ),
            doc_md="## states\n\nFIPS, USPS and names for the 50 states, DC and the territories.",
        ),
        "vgi.result_columns_schema": result_columns(STATES_SCHEMA),
        "vgi.source_url": "https://www.census.gov/library/reference/code-lists/ansi.html",
        "domain": "civic",
        "vgi.executable_examples": json.dumps(_STATES_EXAMPLES),
        "vgi.example_queries": json.dumps(
            [{"description": e["description"], "sql": e["sql"]} for e in _STATES_EXAMPLES]
        ),
    },
)


# ---------------------------------------------------------------- scalars


class NormalizeDistrict(ScalarFunction):
    """``normalize_district(label)``: any district spelling -> the two-digit join key."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "normalize_district"
        description = (
            "Normalize a House district label ('AL', '7', '12.0', 'at-large') to two-digit text ('00' at-large)"
        )
        tags = {
            **object_tags(
                title="Normalize District Number",
                category="reference",
                keywords="district number, at-large, normalize, join key, congressional district, zero pad",
                doc_llm=(
                    "Convert a congressional district label from any source to this worker's join key: two-digit "
                    "`VARCHAR`, '00' for at-large. 'AL', 'at-large', 'At Large', '0', '00' -> '00'; '7', '07', '7.0' "
                    "-> '07'; '98' (Census delegate code) stays '98'; anything unparseable ('ZZ', '') -> NULL. "
                    "Vectorized: apply it to a whole column before joining to house_results or districts()."
                ),
                doc_md=(
                    "## normalize_district(label)\n\n"
                    "`'AL'` -> `'00'`, `'7'` -> `'07'`, `'12.0'` -> `'12'`; unparseable -> NULL."
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Normalize several district spellings",
                        "sql": "SELECT d, us_elections.main.normalize_district(d) AS district "
                        "FROM (VALUES ('AL'), ('7'), ('12.0'), ('ZZ')) t(d) ORDER BY d",
                        "expected_result": [["12.0", "12"], ["7", "07"], ["AL", "00"], ["ZZ", None]],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql="SELECT us_elections.main.normalize_district('AL') AS at_large, "
                "us_elections.main.normalize_district('7') AS seventh",
                description="At-large and single-digit labels",
            ),
        ]

    @classmethod
    def compute(
        cls,
        label: Annotated[pa.StringArray, Param(doc="District label in any common spelling; NULL in, NULL out.")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Normalize every label."""
        return pa.array([normalize_district(v) for v in label.to_pylist()], pa.string())


class CongressForYear(ScalarFunction):
    """``congress_for_year(year)``: the Congress elected at a general election in that year."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "congress_for_year"
        description = "The Congress elected at the regular November election of a year: 2024 -> 119"
        tags = {
            **object_tags(
                title="Congress Number for Election Year",
                category="reference",
                keywords="congress number, election year, 119th congress, session, term",
                doc_llm=(
                    "Return (year - 1786) / 2 rounded down: for an even year, the Congress elected that November "
                    "(2024 -> 119, serving 2025-2027); for an odd year, the Congress sitting that year (2023 -> "
                    "118). Use it to join election years to districts(congress) or Congress member data. "
                    "NULL in, NULL out."
                ),
                doc_md="## congress_for_year(year)\n\n`2024` -> `119`, `1788` -> `1`, `2023` -> `118`.",
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Congress numbers for a few election years",
                        "sql": "SELECT y, us_elections.main.congress_for_year(y) AS congress "
                        "FROM (VALUES (1788), (2022), (2024)) t(y) ORDER BY y",
                        "expected_result": [[1788, 1], [2022, 118], [2024, 119]],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql="SELECT us_elections.main.congress_for_year(2024) AS congress",
                description="The Congress elected in November 2024",
            ),
        ]

    @classmethod
    def compute(
        cls,
        year: Annotated[pa.Int32Array, Param(doc="Election year; NULL in, NULL out.")],
    ) -> Annotated[pa.Int32Array, Returns()]:
        """Compute the Congress number for every year."""
        values = pc.cast(year, pa.int64())
        result = pc.cast(pc.floor(pc.divide(pc.cast(pc.subtract(values, 1786), pa.float64()), 2.0)), pa.int32())
        return result  # type: ignore[no-any-return]


TABLES = [APPORTIONMENT_TABLE, STATES_TABLE]
SCANS: list[type[Any]] = [ApportionmentScan, StatesScan]
SCALARS: list[type[Any]] = [NormalizeDistrict, CongressForYear]
