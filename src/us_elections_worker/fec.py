"""FEC campaign finance from the OpenFEC API (https://api.open.fec.gov/developers/).

Every call needs an api.data.gov key, read from a DuckDB secret::

    CREATE SECRET fec (TYPE us_elections_fec, API_KEY 'your-key');

The key is read from the secret on every request and never stored in scan
state. Results stream one API page (100 rows) per batch; the page cursor is the
only state, so scans resume across HTTP-transport workers. 429 responses are
retried with backoff (api.data.gov allows 1,000 requests per hour per key).
"""

from __future__ import annotations

import urllib.parse
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg, Secret
from vgi.catalog import SecretTypeSpec
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from . import states
from .common import HttpStatusError, UpstreamError, get_json, normalize_district, options_of
from .meta import SOURCE_URL, column, object_tags, result_columns

SECRET_TYPE = "us_elections_fec"
PER_PAGE = 100
MAX_PAGES = 200
FEC_DOCS = "https://api.open.fec.gov/developers/"
MONEY = pa.decimal128(18, 2)

FEC_SECRET_TYPE = SecretTypeSpec(
    name=SECRET_TYPE,
    description="api.data.gov API key for the FEC OpenFEC API (free at https://api.data.gov/signup/).",
    schema=pa.schema(
        [
            pa.field(
                "api_key",
                pa.string(),
                metadata={"redact": "true", "vgi_doc": "api.data.gov key sent as the api_key query parameter."},
            )
        ],
        metadata={"vgi_doc_url": f"{SOURCE_URL}#fec-api-key"},
    ),
)

_NO_KEY = (
    "No FEC API key: the FEC functions need an api.data.gov key (free at https://api.data.gov/signup/). "
    f"Run CREATE SECRET fec (TYPE {SECRET_TYPE}, API_KEY 'your-key') and query again."
)


def api_key(params: ProcessParams[Any]) -> str:
    """Return the FEC key from the DuckDB secret, or fail with instructions."""
    for secret in params.secrets.of_type(SECRET_TYPE):
        value = secret.get("api_key")
        if value is not None and value.as_py():
            return str(value.as_py())
    raise UpstreamError(_NO_KEY)


def fetch_page(base_url: str, endpoint: str, query: dict[str, Any], key: str, page: int) -> dict[str, Any]:
    """GET one page of an OpenFEC list endpoint.

    Args:
        base_url: API root, e.g. https://api.open.fec.gov/v1.
        endpoint: Path such as '/candidates/'.
        query: Filters; list values repeat the parameter.
        key: api.data.gov key.
        page: 1-based page number.

    Returns:
        The decoded JSON body.

    Raises:
        HttpStatusError: The API refused the request.
        UpstreamError: The API refused the key, or the response is malformed.

    """
    params = [(k, v) for k, vs in query.items() if vs is not None for v in (vs if isinstance(vs, list) else [vs])]
    params += [("per_page", PER_PAGE), ("page", page), ("api_key", key)]
    url = f"{base_url}{endpoint}?{urllib.parse.urlencode(params)}"
    try:
        body = get_json(url)
    except HttpStatusError as err:
        if err.status == 403 and b"API_KEY" in err.body:
            raise UpstreamError(
                f"the FEC API rejected the API key (HTTP 403): {err.body[:200].decode(errors='replace')}. "
                f"Check the {SECRET_TYPE} secret."
            ) from err
        raise
    if not isinstance(body, dict) or "results" not in body:
        raise UpstreamError(f"unexpected FEC response from {endpoint}: {str(body)[:200]}")
    return body


# ---------------------------------------------------------------- value helpers


def _money(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except InvalidOperation:
        return None


def _date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _ints(value: Any) -> list[int] | None:
    if value is None:
        return None
    return [int(v) for v in value if v is not None]


def _strs(value: Any) -> list[str] | None:
    if value is None:
        return None
    return [str(v) for v in value if v is not None]


def _int(value: Any) -> int | None:
    try:
        return int(value) if value is not None and value != "" else None
    except TypeError, ValueError:
        return None


def _upper(value: str | None) -> str | None:
    return value.strip().upper() if value and value.strip() else None


def _state_code(value: str | None) -> str | None:
    found = states.resolve(value)
    return found[1] if found else None


# ---------------------------------------------------------------- the shared paginated scan


@dataclass(kw_only=True)
class PageCursor(ArrowSerializableDataclass):
    """Next page to request; 0 when done."""

    page: int = 1


class _FecScan:
    """Mixin: page through an OpenFEC endpoint, one page per batch."""

    ENDPOINT: ClassVar[str] = ""
    SCHEMA: ClassVar[pa.Schema]

    @classmethod
    def query(cls, args: Any) -> dict[str, Any]:
        """Translate the SQL arguments into API filters."""
        raise NotImplementedError

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map one API result to an output row."""
        raise NotImplementedError

    @classmethod
    def initial_state(cls, params: ProcessParams[Any]) -> PageCursor:
        """Check the key and the filters before the first request."""
        api_key(params)
        cls.query(params.args)
        return PageCursor()

    @classmethod
    def process(cls, params: ProcessParams[Any], state: PageCursor, out: OutputCollector) -> None:
        """Fetch one page and emit it; finish after the last page."""
        if state.page == 0:
            out.finish()
            return
        body = fetch_page(
            options_of(params).fec_base_url, cls.ENDPOINT, cls.query(params.args), api_key(params), state.page
        )
        rows = [cls.row(item) for item in body.get("results") or []]
        pages = int((body.get("pagination") or {}).get("pages") or 0)
        state.page = state.page + 1 if rows and state.page < min(pages, MAX_PAGES) else 0
        columns = {name: [r.get(name) for r in rows] for name in cls.SCHEMA.names}
        batch = pa.RecordBatch.from_pydict(columns, schema=cls.SCHEMA)
        out.emit(batch.select(params.output_schema.names))


# ---------------------------------------------------------------- fec_candidates


def _candidate_fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c("candidate_id", pa.string(), "FEC candidate id ('H4DE01084'); join key across FEC tables.", nullable=False),
        c("name", pa.string(), "Candidate name as filed ('LAST, FIRST MIDDLE')."),
        c("party", pa.string(), "FEC party code ('DEM', 'REP', 'LIB', 'IND' ...)."),
        c("party_full", pa.string(), "Party name ('DEMOCRATIC PARTY')."),
        c("office", pa.string(), "Office sought: 'H' House, 'S' Senate, 'P' President."),
        c("state", pa.string(), "USPS state code of the race ('US' for president)."),
        c("district", pa.string(), "Two-digit House district ('00' at-large and for Senate/President)."),
        c("incumbent_challenge", pa.string(), "'I' incumbent, 'C' challenger, 'O' open seat."),
        c("candidate_status", pa.string(), "'C' statutory candidate, 'F' future, 'N' not yet, 'P' prior."),
        c("candidate_inactive", pa.bool_(), "TRUE when the FEC marks the candidate inactive."),
        c("election_years", pa.list_(pa.int32()), "Election years the candidate registered for."),
        c("cycles", pa.list_(pa.int32()), "Two-year cycles with activity."),
        c("active_through", pa.int32(), "Last election year the candidate is active through."),
        c("has_raised_funds", pa.bool_(), "TRUE when the candidate's committees reported receipts."),
        c("first_file_date", pa.date32(), "Date of the first filing."),
        c("last_file_date", pa.date32(), "Date of the latest filing."),
    ]


CANDIDATE_SCHEMA = pa.schema(_candidate_fields())


@dataclass(slots=True, frozen=True, kw_only=True)
class CandidateArgs:
    """Arguments for ``fec_candidates(...)``."""

    election_year: Annotated[
        int | None, Arg("election_year", default=None, doc="Election year, e.g. 2024.", ge=1976, le=2100)
    ]
    state: Annotated[str | None, Arg("state", default=None, doc="State: USPS code, FIPS code or name.")]
    office: Annotated[str | None, Arg("office", default=None, doc="'H', 'S' or 'P'.", choices=["H", "S", "P"])]
    district: Annotated[
        str | None,
        Arg(
            "district",
            default=None,
            doc="House district number in any common spelling; normalized like normalize_district.",
        ),
    ]
    party: Annotated[str | None, Arg("party", default=None, doc="Three-letter FEC party code (DEM for Democratic).")]
    name: Annotated[str | None, Arg("name", default=None, doc="Full-text name search, e.g. 'rochester'.")]
    candidate_id: Annotated[str | None, Arg("candidate_id", default=None, doc="One FEC candidate id.")]


def _candidate_query(a: Any) -> dict[str, Any]:
    if not any((a.election_year, a.state, a.name, a.candidate_id)):
        raise ValueError("fec_candidates needs at least one of election_year, state, name or candidate_id")
    return {
        "election_year": a.election_year,
        "state": _state_code(a.state),
        "office": a.office,
        "district": normalize_district(a.district) if a.district else None,
        "party": _upper(a.party),
        "q": a.name.strip() if a.name else None,
        "candidate_id": _upper(a.candidate_id),
        "sort": "name",
    }


def _candidate_row(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": item.get("candidate_id"),
        "name": item.get("name"),
        "party": item.get("party"),
        "party_full": item.get("party_full"),
        "office": item.get("office"),
        "state": item.get("state"),
        "district": normalize_district(item.get("district")),
        "incumbent_challenge": item.get("incumbent_challenge"),
        "candidate_status": item.get("candidate_status"),
        "candidate_inactive": item.get("candidate_inactive"),
        "election_years": _ints(item.get("election_years")),
        "cycles": _ints(item.get("cycles")),
        "active_through": _int(item.get("active_through")),
        "has_raised_funds": item.get("has_raised_funds"),
        "first_file_date": _date(item.get("first_file_date")),
        "last_file_date": _date(item.get("last_file_date")),
    }


_KEY_NOTE = f" Needs CREATE SECRET (TYPE {SECRET_TYPE}, API_KEY '...') with a free api.data.gov key."


@init_single_worker
class FecCandidates(_FecScan, TableFunctionGenerator[CandidateArgs, PageCursor]):
    """FEC candidate registrations."""

    ENDPOINT: ClassVar[str] = "/candidates/"
    SCHEMA: ClassVar[pa.Schema] = CANDIDATE_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "fec_candidates"
        description = "Candidates registered with the FEC, filtered by year, state, office, district, party or name"
        projection_pushdown = True
        tags = {
            **object_tags(
                title="FEC Candidates",
                category="finance",
                keywords="fec, candidates, candidate id, campaign registration, federal candidates, openfec",
                doc_llm=(
                    "Candidates registered with the Federal Election Commission, from the OpenFEC /candidates/ "
                    "endpoint. Named filters (at least one of election_year, state, name, candidate_id): "
                    "election_year := 2024, state := 'DE', office := 'H'|'S'|'P', district := '01' (or 'AL'), "
                    "party := 'DEM', name := 'rochester' (full-text), candidate_id := 'H4DE01084'. All filters "
                    "are sent to the API. Columns: candidate_id, name, party, party_full, office, state, district "
                    "(two-digit, '00' at-large), incumbent_challenge, candidate_status, candidate_inactive, "
                    "election_years, cycles, active_through, has_raised_funds, first_file_date, last_file_date."
                    + _KEY_NOTE
                ),
                doc_md=(
                    "## fec_candidates(election_year := , state := , office := , district := , party := , name := , "
                    "candidate_id := )\n\nFEC candidate registrations ([OpenFEC](" + FEC_DOCS + ")). "
                    "Needs a `us_elections_fec` secret."
                ),
            ),
            "vgi.result_columns_schema": result_columns(CANDIDATE_SCHEMA),
            "vgi.source_url": FEC_DOCS,
        }
        examples = [
            FunctionExample(
                sql="SELECT candidate_id, name, party, office, district FROM us_elections.main.fec_candidates("
                "election_year := 2024, state := 'DE') WHERE has_raised_funds ORDER BY office, name",
                description="Delaware's 2024 federal candidates who raised money",
            ),
        ]

    @classmethod
    def on_bind(
        cls,
        params: BindParams[CandidateArgs],
        *,
        us_elections_fec: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Declare the output schema and ask DuckDB for the ``us_elections_fec`` secret."""
        return BindResponse(output_schema=CANDIDATE_SCHEMA)

    @classmethod
    def query(cls, args: Any) -> dict[str, Any]:
        """Translate the SQL arguments into API filters."""
        return _candidate_query(args)

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map one API result to an output row."""
        return _candidate_row(item)


# ---------------------------------------------------------------- fec_candidate_totals


def _totals_fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c("candidate_id", pa.string(), "FEC candidate id.", nullable=False),
        c("name", pa.string(), "Candidate name as filed."),
        c("party", pa.string(), "FEC party code."),
        c("office", pa.string(), "'H', 'S' or 'P'."),
        c("state", pa.string(), "USPS state code of the race."),
        c("district", pa.string(), "Two-digit House district ('00' at-large and for Senate/President)."),
        c("election_year", pa.int32(), "The election these totals are for."),
        c("cycle", pa.int32(), "Two-year FEC cycle of the totals record."),
        c("incumbent_challenge", pa.string(), "'I' incumbent, 'C' challenger, 'O' open seat."),
        c("receipts", MONEY, "Total receipts in US dollars over the election period."),
        c("disbursements", MONEY, "Total disbursements in US dollars."),
        c("cash_on_hand_end_period", MONEY, "Cash on hand at the end of the coverage period."),
        c("debts_owed_by_committee", MONEY, "Debts owed by the campaign committee."),
        c("individual_itemized_contributions", MONEY, "Itemized contributions from individuals (over $200)."),
        c("other_political_committee_contributions", MONEY, "Contributions from PACs and other committees."),
        c("transfers_from_other_authorized_committee", MONEY, "Transfers from the candidate's other committees."),
        c("coverage_start_date", pa.date32(), "First day covered by the reports."),
        c("coverage_end_date", pa.date32(), "Last day covered by the reports."),
    ]


TOTALS_SCHEMA = pa.schema(_totals_fields())


@dataclass(slots=True, frozen=True, kw_only=True)
class TotalsArgs:
    """Arguments for ``fec_candidate_totals(election_year, ...)``."""

    election_year: Annotated[
        int, Arg(0, doc="Election year, e.g. 2024 (totals cover the whole election period).", ge=1976, le=2100)
    ]
    state: Annotated[str | None, Arg("state", default=None, doc="State: USPS code, FIPS code or name.")]
    office: Annotated[str | None, Arg("office", default=None, doc="'H', 'S' or 'P'.", choices=["H", "S", "P"])]
    district: Annotated[
        str | None,
        Arg(
            "district",
            default=None,
            doc="House district number in any common spelling; normalized like normalize_district.",
        ),
    ]
    party: Annotated[str | None, Arg("party", default=None, doc="Three-letter FEC party code (DEM for Democratic).")]
    candidate_id: Annotated[str | None, Arg("candidate_id", default=None, doc="One FEC candidate id.")]


@init_single_worker
class FecCandidateTotals(_FecScan, TableFunctionGenerator[TotalsArgs, PageCursor]):
    """Per-candidate fundraising totals for an election."""

    ENDPOINT: ClassVar[str] = "/candidates/totals/"
    SCHEMA: ClassVar[pa.Schema] = TOTALS_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "fec_candidate_totals"
        description = "Receipts, spending, cash and debts per FEC candidate for one election year"
        projection_pushdown = True
        tags = {
            **object_tags(
                title="FEC Candidate Financial Totals",
                category="finance",
                keywords="fec, fundraising, receipts, disbursements, cash on hand, campaign money, totals",
                doc_llm=(
                    "Financial totals per candidate for one election, from OpenFEC /candidates/totals/ "
                    "(election_full = true: the whole election period, six years for Senate). Positional: "
                    "election_year (`INTEGER`). Named filters, sent to the API: state := 'DE', office := 'H'|'S'|'P', "
                    "district := '01', party := 'DEM', candidate_id := 'S4DE00060'. Money columns are "
                    "`DECIMAL(18,2)` US dollars: receipts, disbursements, cash_on_hand_end_period, "
                    "debts_owed_by_committee, individual_itemized_contributions, "
                    "other_political_committee_contributions, transfers_from_other_authorized_committee. Join "
                    "to house_results or senate_results on (year = election_year, state_po = state, district) and "
                    "the candidate's name." + _KEY_NOTE
                ),
                doc_md=(
                    "## fec_candidate_totals(election_year, state := , office := , district := , party := , "
                    "candidate_id := )\n\nFundraising totals per candidate ([OpenFEC](" + FEC_DOCS + ")). "
                    "Needs a `us_elections_fec` secret."
                ),
            ),
            "vgi.result_columns_schema": result_columns(TOTALS_SCHEMA),
            "vgi.source_url": FEC_DOCS,
        }
        examples = [
            FunctionExample(
                sql="SELECT name, party, office, receipts, disbursements FROM us_elections.main.fec_candidate_totals("
                "2024, state := 'DE') ORDER BY receipts DESC NULLS LAST LIMIT 5",
                description="Top fundraisers in Delaware's 2024 federal races",
            ),
        ]

    @classmethod
    def on_bind(
        cls,
        params: BindParams[TotalsArgs],
        *,
        us_elections_fec: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Declare the output schema and ask DuckDB for the ``us_elections_fec`` secret."""
        return BindResponse(output_schema=TOTALS_SCHEMA)

    @classmethod
    def query(cls, args: Any) -> dict[str, Any]:
        """Translate the SQL arguments into API filters."""
        return {
            "election_year": args.election_year,
            "election_full": "true",
            "state": _state_code(args.state),
            "office": args.office,
            "district": normalize_district(args.district) if args.district else None,
            "party": _upper(args.party),
            "candidate_id": _upper(args.candidate_id),
            "sort": "-receipts",
        }

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map one API result to an output row."""
        money = [f.name for f in TOTALS_SCHEMA if f.type == MONEY]
        return {
            "candidate_id": item.get("candidate_id"),
            "name": item.get("name"),
            "party": item.get("party"),
            "office": item.get("office"),
            "state": item.get("state"),
            "district": normalize_district(item.get("district")),
            "election_year": _int(item.get("candidate_election_year") or item.get("election_year")),
            "cycle": _int(item.get("cycle")),
            "incumbent_challenge": item.get("incumbent_challenge"),
            **{m: _money(item.get(m)) for m in money},
            "coverage_start_date": _date(item.get("coverage_start_date")),
            "coverage_end_date": _date(item.get("coverage_end_date")),
        }


# ---------------------------------------------------------------- fec_committees


def _committee_fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c("committee_id", pa.string(), "FEC committee id ('C00884494').", nullable=False),
        c("name", pa.string(), "Committee name."),
        c("committee_type", pa.string(), "FEC type code: 'H'/'S'/'P' candidate, 'O' super PAC, 'Q'/'N' PAC ..."),
        c("committee_type_full", pa.string(), "Committee type in words ('Super PAC (Independent Expenditure-Only)')."),
        c(
            "designation",
            pa.string(),
            "'P' principal campaign, 'A' authorized, 'J' joint fundraiser, 'U' unauthorized ...",
        ),
        c("designation_full", pa.string(), "Designation in words."),
        c("party", pa.string(), "Party code, when the committee has one."),
        c("state", pa.string(), "USPS code of the committee's state."),
        c("organization_type", pa.string(), "Organization type code (corporation, labor ...), when reported."),
        c("filing_frequency", pa.string(), "'Q' quarterly, 'M' monthly, 'T' terminated ..."),
        c("treasurer_name", pa.string(), "Treasurer of record."),
        c("candidate_ids", pa.list_(pa.string()), "Candidates the committee is linked to (FEC candidate ids)."),
        c("cycles", pa.list_(pa.int32()), "Two-year cycles with activity."),
        c("first_file_date", pa.date32(), "Date of the first filing."),
        c("last_file_date", pa.date32(), "Date of the latest filing."),
    ]


COMMITTEE_SCHEMA = pa.schema(_committee_fields())


@dataclass(slots=True, frozen=True, kw_only=True)
class CommitteeArgs:
    """Arguments for ``fec_committees(...)``."""

    cycle: Annotated[int | None, Arg("cycle", default=None, doc="Two-year cycle, e.g. 2024.", ge=1976, le=2100)]
    state: Annotated[str | None, Arg("state", default=None, doc="State: USPS code, FIPS code or name.")]
    committee_type: Annotated[
        str | None,
        Arg(
            "committee_type",
            default=None,
            doc="One-letter FEC committee type code, as in the committee_type output column.",
        ),
    ]
    designation: Annotated[
        str | None,
        Arg("designation", default=None, doc="One-letter FEC designation code, as in the designation output column."),
    ]
    party: Annotated[str | None, Arg("party", default=None, doc="Three-letter FEC party code (DEM for Democratic).")]
    name: Annotated[str | None, Arg("name", default=None, doc="Full-text name search.")]
    candidate_id: Annotated[str | None, Arg("candidate_id", default=None, doc="Committees linked to this candidate.")]


@init_single_worker
class FecCommittees(_FecScan, TableFunctionGenerator[CommitteeArgs, PageCursor]):
    """FEC-registered committees."""

    ENDPOINT: ClassVar[str] = "/committees/"
    SCHEMA: ClassVar[pa.Schema] = COMMITTEE_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "fec_committees"
        description = "Campaign committees, PACs and super PACs registered with the FEC"
        projection_pushdown = True
        tags = {
            **object_tags(
                title="FEC Committees",
                category="finance",
                keywords="fec, committees, pac, super pac, campaign committee, treasurer, openfec",
                doc_llm=(
                    "Committees registered with the FEC (campaign committees, PACs, super PACs, party committees) "
                    "from OpenFEC /committees/. Named filters (at least one of cycle, state, name, candidate_id), "
                    "all sent to the API: cycle := 2024, state := 'DE', committee_type := 'O' (super PAC), "
                    "designation := 'P' (principal campaign committee), party := 'DEM', name := 'victory', "
                    "candidate_id := 'S4DE00060'. Columns: committee_id, name, committee_type(_full), "
                    "designation(_full), party, state, organization_type, filing_frequency, treasurer_name, "
                    "candidate_ids (`VARCHAR[]`), cycles, first_file_date, last_file_date." + _KEY_NOTE
                ),
                doc_md=(
                    "## fec_committees(cycle := , state := , committee_type := , designation := , party := , "
                    "name := , candidate_id := )\n\nFEC committees ([OpenFEC](" + FEC_DOCS + ")). "
                    "Needs a `us_elections_fec` secret."
                ),
            ),
            "vgi.result_columns_schema": result_columns(COMMITTEE_SCHEMA),
            "vgi.source_url": FEC_DOCS,
        }
        examples = [
            FunctionExample(
                sql="SELECT committee_type_full, count(*) AS committees FROM us_elections.main.fec_committees("
                "cycle := 2024, state := 'DE') GROUP BY ALL ORDER BY committees DESC",
                description="Kinds of committees registered in Delaware for 2024",
            ),
        ]

    @classmethod
    def on_bind(
        cls,
        params: BindParams[CommitteeArgs],
        *,
        us_elections_fec: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Declare the output schema and ask DuckDB for the ``us_elections_fec`` secret."""
        return BindResponse(output_schema=COMMITTEE_SCHEMA)

    @classmethod
    def query(cls, args: Any) -> dict[str, Any]:
        """Translate the SQL arguments into API filters."""
        if not any((args.cycle, args.state, args.name, args.candidate_id)):
            raise ValueError("fec_committees needs at least one of cycle, state, name or candidate_id")
        return {
            "cycle": args.cycle,
            "state": _state_code(args.state),
            "committee_type": _upper(args.committee_type),
            "designation": _upper(args.designation),
            "party": _upper(args.party),
            "q": args.name.strip() if args.name else None,
            "candidate_id": _upper(args.candidate_id),
            "sort": "name",
        }

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map one API result to an output row."""
        return {
            "committee_id": item.get("committee_id"),
            "name": item.get("name"),
            "committee_type": item.get("committee_type"),
            "committee_type_full": item.get("committee_type_full"),
            "designation": item.get("designation"),
            "designation_full": item.get("designation_full"),
            "party": item.get("party"),
            "state": item.get("state"),
            "organization_type": item.get("organization_type"),
            "filing_frequency": item.get("filing_frequency"),
            "treasurer_name": item.get("treasurer_name"),
            "candidate_ids": _strs(item.get("candidate_ids")),
            "cycles": _ints(item.get("cycles")),
            "first_file_date": _date(item.get("first_file_date")),
            "last_file_date": _date(item.get("last_file_date")),
        }


# ---------------------------------------------------------------- fec_contributions_by_state


def _by_state_fields() -> list[pa.Field[Any]]:
    c = column
    return [
        c("candidate_id", pa.string(), "FEC candidate id.", nullable=False),
        c("cycle", pa.int32(), "Two-year cycle requested (election_full: the whole election period)."),
        c("state", pa.string(), "USPS code of the contributors' state."),
        c("state_full", pa.string(), "Contributors' state name."),
        c("total", MONEY, "Itemized individual contributions from that state, US dollars."),
        c("contributions", pa.int64(), "Number of itemized contributions from that state."),
    ]


BY_STATE_SCHEMA = pa.schema(_by_state_fields())


@dataclass(slots=True, frozen=True, kw_only=True)
class ByStateArgs:
    """Arguments for ``fec_contributions_by_state(candidate_id, cycle)``."""

    candidate_id: Annotated[str, Arg(0, doc="FEC candidate id, e.g. 'S4DE00060'.", pattern=r"^[HSPhsp]\w{8}$")]
    cycle: Annotated[int, Arg(1, doc="Election cycle (even year), e.g. 2024.", ge=1976, le=2100)]


@init_single_worker
class FecContributionsByState(_FecScan, TableFunctionGenerator[ByStateArgs, PageCursor]):
    """Where a candidate's itemized individual contributions came from."""

    ENDPOINT: ClassVar[str] = "/schedules/schedule_a/by_state/by_candidate/"
    SCHEMA: ClassVar[pa.Schema] = BY_STATE_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "fec_contributions_by_state"
        description = "A candidate's itemized individual contributions summed by contributor state"
        projection_pushdown = True
        tags = {
            **object_tags(
                title="FEC Contributions by State",
                category="finance",
                keywords="fec, schedule a, contributions by state, donor geography, out-of-state money, receipts",
                doc_llm=(
                    "Itemized individual contributions (Schedule A, over $200) to one candidate's committees, "
                    "summed by the contributors' state, from OpenFEC /schedules/schedule_a/by_state/by_candidate/ "
                    "with election_full = true. Positional: candidate_id (`VARCHAR`, from fec_candidates), cycle "
                    "(`INTEGER` even year). Columns: candidate_id, cycle, state, state_full, total (`DECIMAL(18,2)` "
                    "US dollars), contributions (count). Compare the home state's total with the rest to measure "
                    "out-of-state money." + _KEY_NOTE
                ),
                doc_md=(
                    "## fec_contributions_by_state(candidate_id, cycle)\n\nItemized contributions to a candidate "
                    "by donor state ([OpenFEC](" + FEC_DOCS + ")). Needs a `us_elections_fec` secret."
                ),
            ),
            "vgi.result_columns_schema": result_columns(BY_STATE_SCHEMA),
            "vgi.source_url": FEC_DOCS,
        }
        examples = [
            FunctionExample(
                sql="SELECT state, total, contributions FROM us_elections.main.fec_contributions_by_state("
                "'S4DE00060', 2024) ORDER BY total DESC LIMIT 5",
                description="Top donor states for Lisa Blunt Rochester's 2024 Senate campaign",
            ),
        ]

    @classmethod
    def on_bind(
        cls,
        params: BindParams[ByStateArgs],
        *,
        us_elections_fec: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Declare the output schema and ask DuckDB for the ``us_elections_fec`` secret."""
        return BindResponse(output_schema=BY_STATE_SCHEMA)

    @classmethod
    def query(cls, args: Any) -> dict[str, Any]:
        """Translate the SQL arguments into API filters."""
        return {"candidate_id": args.candidate_id.upper(), "cycle": args.cycle, "election_full": "true"}

    @classmethod
    def row(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Map one API result to an output row."""
        return {
            "candidate_id": item.get("candidate_id"),
            "cycle": _int(item.get("cycle")),
            "state": item.get("state"),
            "state_full": item.get("state_full"),
            "total": _money(item.get("total")),
            "contributions": _int(item.get("count")),
        }


FUNCTIONS: list[type[Any]] = [FecCandidates, FecCandidateTotals, FecCommittees, FecContributionsByState]
