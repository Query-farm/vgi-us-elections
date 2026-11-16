<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query US Election Results, District Maps and Campaign Finance in DuckDB

> **vgi-us-elections** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-us-elections/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-us-elections/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

US federal elections as SQL tables: certified House, Senate and presidential returns since 1976
and county-level presidential returns since 2000 from the
[MIT Election Data + Science Lab](https://electionlab.mit.edu/data), House seats per state after
every census since 1910, congressional district polygons for **every Congress from 1789 to
today**, and FEC campaign finance. Everything shares the same join keys, so results, maps and
money combine in one query.

```sql
-- Who won each Senate race in 2024, and by how much?
SELECT state_po, candidate, party_simplified, round(100 * vote_share, 1) AS pct
FROM us_elections.main.senate_results
WHERE year = 2024 AND stage = 'gen' AND NOT special
QUALIFY row_number() OVER (PARTITION BY state_po ORDER BY candidatevotes DESC) = 1
ORDER BY pct;
```

## Quick start

**DuckDB 2.0:**

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;

ATTACH 'us_elections' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-us-elections@v0.1.0 vgi-us-elections-worker');

SELECT census_year, seats FROM us_elections.main.apportionment WHERE state_po = 'CA' ORDER BY census_year;
```

**Haybarn 2.0** (`uvx haybarn-cli`): `INSTALL vgi FROM community; LOAD vgi;` then the same `ATTACH`.

From a checkout: `make build`, then use `LOCATION '/abs/path/to/vgi-us-elections/.venv/bin/vgi-us-elections-worker'`.

## Tables and functions

| Object | Kind | What it returns | Key |
|---|---|---|---|
| `house_results` | table | US House returns by district, 1976-2024 (MEDSL, CC0) | guestbook* |
| `senate_results` | table | US Senate returns by state, 1976-2024 | keyless |
| `president_results` | table | Presidential returns by state, 1976-2024 | keyless |
| `county_president_results` | table | Presidential returns by county (five-digit FIPS), 2000-2024 | guestbook* |
| `apportionment` | table | House seats, population and seat change per state per census, 1910-2020 (bundled) | keyless |
| `states` | table | FIPS / USPS / name for the states, DC and territories (bundled) | keyless |
| `districts(congress, state := NULL)` | table function | District polygons for any Congress, 1st-119th (Lewis et al., UCLA) | keyless |
| `current_districts(state := NULL, resolution := '500k')` | table function | 119th Congress districts from Census cartographic boundaries, with land/water area | keyless |
| `fec_candidates(election_year := , state := , office := , district := , party := , name := , candidate_id := )` | table function | FEC candidate registrations | FEC key |
| `fec_candidate_totals(election_year, state := , office := , district := , party := , candidate_id := )` | table function | Receipts, disbursements, cash and debts per candidate | FEC key |
| `fec_committees(cycle := , state := , committee_type := , designation := , party := , name := , candidate_id := )` | table function | Campaign committees, PACs and super PACs | FEC key |
| `fec_contributions_by_state(candidate_id, cycle)` | table function | A candidate's itemized individual contributions summed by donor state | FEC key |
| `normalize_district(label)` | scalar | Any district spelling (`'AL'`, `'7'`, `'12.0'`) to the two-digit key | n/a |
| `congress_for_year(year)` | scalar | The Congress elected in a year's November election (2024 -> 119) | n/a |
| `cache_info()` | table function | What is cached, its upstream version, and where to drop manual downloads | n/a |

\* see [The guestbook-gated files](#the-guestbook-gated-files).

Every result row carries a derived `vote_share` (`candidatevotes / totalvotes`); House and Senate
rows carry a derived `congress`; the House file's `party_simplified` is derived from its party
labels with MEDSL's four groups.

### Join keys

| Key | Type | Convention |
|---|---|---|
| `year` | `INTEGER` | election year; FEC `election_year` / `cycle` |
| `state_po` | `VARCHAR` | USPS code (`'DE'`), the same code FEC uses |
| `state_fips` | `VARCHAR` | two digits with the leading zero (`'06'`) |
| `district` | `VARCHAR` | two digits, **`'00'` = at-large**; `'98'` = non-voting delegate (Census). Use `normalize_district()` for other spellings. Senate and presidential tables have no district. |
| `congress` | `INTEGER` | 1 (1789) ... 119 (2025-2027); `congress_for_year(year)` |
| `candidate_id` | `VARCHAR` | FEC candidate id (`'S4DE00060'`) |
| `county_fips` | `VARCHAR` | five digits (`'01001'`) |

Member-level data (bioguide ids, roll-call votes) lives in a separate Congress worker; join it to
`districts()` / `house_results` on `(congress, state_po, district)`. MEDSL results carry no FEC
ids, so match results to FEC on `(year, state_po, district, office)` plus the candidate name.

```sql
-- 2024 House result for each Wyoming district, with its polygon (spatial extension).
LOAD spatial;
SELECT r.candidate, r.vote_share, ST_Area(d.geometry) AS area_deg2
FROM us_elections.main.house_results r
JOIN us_elections.main.current_districts(state := 'WY') d
  ON d.congress = r.congress AND d.state_po = r.state_po AND d.district = r.district
WHERE r.year = 2024 AND r.stage = 'gen';
```

Geometry columns are `GEOMETRY('OGC:CRS84')` (lon/lat). Both sources publish NAD83, which is
passed through unchanged (about a metre from WGS84).

## What pushes down

- **Result tables**: `WHERE` comparisons and `IN` lists on `year`, `state_po`, `state_fips`,
  `district`, `congress`, `county_fips` and `stage` are applied inside the worker before rows are
  sent; every other predicate is applied exactly too (the SDK re-checks all pushed filters).
  Only the projected columns cross the wire. The data itself comes from the cached file, so there
  is no upstream request per query.
- **`districts()`**: `congress` and `state` choose which per-state files are downloaded. Without
  `state` every state's file for that Congress is fetched (tens of MB the first time).
- **`current_districts()`**: `resolution` picks the file; `state` filters in the worker.
- **FEC functions**: every named argument becomes an OpenFEC query parameter. Results stream one
  100-row API page per batch; the page number is the only scan state, so scans resume across
  HTTP-transport workers.

## The download cache

Nothing is downloaded at import or ATTACH. On first use a relation fetches what it needs into
the cache directory (`CACHE_DIR` ATTACH option, default `~/Library/Caches/vgi-us-elections` or
`~/.cache/vgi-us-elections`) and reuses it:

- MEDSL files are resolved through the Dataverse API to the dataset's latest published version,
  header-checked, and re-checked once they are older than `MAX_AGE_DAYS` (default 30). A failed
  re-check keeps serving the cached copy.
- Lewis files are pinned to one git commit and verified against the bundled git blob hashes.
- `OFFLINE true` never downloads; `cache_info()` shows what is present.

| ATTACH option | Default | Purpose |
|---|---|---|
| `CACHE_DIR` | per-user cache dir | where downloads live |
| `MAX_AGE_DAYS` | `30` | re-check interval for MEDSL releases |
| `OFFLINE` | `false` | cached files only |
| `DATAVERSE_URL`, `LEWIS_BASE_URL`, `CENSUS_CD_BASE_URL`, `FEC_BASE_URL` | the public services | mirrors (the tests point these at a local fixture server) |
| `GUESTBOOK_NAME`, `GUESTBOOK_EMAIL`, `GUESTBOOK_INSTITUTION`, `GUESTBOOK_POSITION` | none | your answers to the Dataverse guestbook |
| `DATAVERSE_API_TOKEN` | none | optional Dataverse API token (secret option, redacted) |

### The guestbook-gated files

Harvard Dataverse requires a guestbook response (name, email, institution, position) before it
serves the House and county files; the Senate and President files have none. The data is CC0,
but the worker never invents an identity, so `house_results` and `county_president_results` fail
with instructions until you either:

```sql
ATTACH 'us_elections' (TYPE vgi, LOCATION '...',
  GUESTBOOK_NAME 'Ada Lovelace', GUESTBOOK_EMAIL 'ada@example.org',
  GUESTBOOK_INSTITUTION 'Analytical Engines', GUESTBOOK_POSITION 'Analyst');
```

or download the data file from the dataset page
([House](https://doi.org/10.7910/DVN/IG0UN2), [county](https://doi.org/10.7910/DVN/VOQCHQ)) in a
browser and save it as the `path` that `cache_info()` reports (`.../medsl/house.csv`,
`.../medsl/county_president.csv`). Files you place there are used as-is and never refreshed.

The guestbook submission follows the Dataverse API guide (a `POST` with a `guestbookResponse`
returning a signed URL). It is tested against a local mock; it has **not** been exercised against
harvard.edu, because doing so requires a real person's details.

## FEC API key

The FEC functions need a free [api.data.gov key](https://api.data.gov/signup/):

```sql
CREATE SECRET fec (TYPE us_elections_fec, API_KEY 'your-key');
SELECT name, party, receipts FROM us_elections.main.fec_candidate_totals(2024, state := 'DE', office := 'S');
```

Without the secret they fail with that instruction; everything else works keyless. Requests
retry 429/5xx with backoff and honour `Retry-After` (api.data.gov allows 1,000 requests/hour).

## Testing

```bash
make check        # ruff, mypy, pydoclint, pytest, SQL tests over stdio/launch/http, vgi-lint
make test-unit    # pytest only
make test-sql     # SQL tests through the real vgi extension (stdio)
make live-check   # optional: a few live queries against the real services (network)
```

The tests run offline against `test/mock_api/server.py`, which serves recorded upstream
responses from `test/fixtures/` (see `test/fixtures/README.md`; the House and county rows are
synthetic) and rate-limits every path once to exercise the retry path. The SQL tests run through
Haybarn 2.0's sqllogictest runner with the `vgi` community extension.

## Data terms

The code is 0BSD; the data is not covered by it. MEDSL data: CC0 1.0. Census apportionment and
boundary files: US-government works, public domain. Lewis et al. district boundaries: MIT
licence. FEC data: public domain, but federal law (52 U.S.C. § 30111(a)(4)) forbids using
individual contributors' names and addresses to solicit contributions or for commercial
purposes; this worker exposes only per-state aggregates of contributions. Cite MEDSL and Lewis et
al. when you publish results. Full provenance, versions and checksums:
[DATA-SOURCES.md](DATA-SOURCES.md).

## Support

Open an issue on this repository. For VGI itself, see [query.farm](https://query.farm).

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Released under the [BSD Zero Clause License](LICENSE) (0BSD). Built from the Query.Farm
[vgi-starter-python](https://github.com/Query-farm/vgi-starter-python) template.
