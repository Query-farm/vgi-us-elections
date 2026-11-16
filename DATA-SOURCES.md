# Data sources

All upstream data was checked on 2026-11-15. The code licence (0BSD) does not cover the data;
each source keeps its own terms below.

## Bundled in the package (`src/us_elections_worker/data/`)

| File | Source | Licence | Retrieved | SHA-256 |
|---|---|---|---|---|
| `apportionment.csv` | US Census Bureau, *Historical Apportionment Data*, <https://www2.census.gov/programs-surveys/decennial/2020/data/apportionment/apportionment.csv> (page: <https://www.census.gov/data/tables/time-series/dec/apportionment-data-text.html>), unmodified | US-government work, public domain (17 U.S.C. § 105) | 2026-11-15 | `d9be9c833ad936ee309a83fa6428a3ae2f82d5d8e5b2c69c4d6ff1e79ba57fcd` |
| `lewis_index.json` | File listing (name, congress range, git blob id, size) of `GeoJson/` in <https://github.com/JeffreyBLewis/congressional-district-boundaries> at commit `3c663a8098b58156601e0893f0e67b502f7f8354`, from the GitHub tree API | MIT (Copyright 2013-2018 Jeffrey B. Lewis) | 2026-11-15 | `b96848dec53459a521b8a9e9ce453c95fd74fbc9acd87ea6107f9549351a6372` |

The Census CSV covers the 1910-2020 censuses only. Seat counts for 1790-1900 are published only
as PDF tables and are not included.

## Downloaded at runtime into the cache

Nothing is downloaded at import or ATTACH time. `cache_info()` lists what is cached.

| Data | Source | Licence | Size | Verification |
|---|---|---|---|---|
| U.S. House 1976-2024 | MIT Election Data and Science Lab, Harvard Dataverse [doi:10.7910/DVN/IG0UN2](https://doi.org/10.7910/DVN/IG0UN2) (v15.0 on 2026-11-15) | CC0 1.0 | ~4 MB | header check; SHA-256 recorded |
| U.S. Senate 1976-2024 | MEDSL, [doi:10.7910/DVN/PEJ5QU](https://doi.org/10.7910/DVN/PEJ5QU) (v8.0) | CC0 1.0 | 0.5 MB | header check; SHA-256 recorded |
| U.S. President 1976-2024 | MEDSL, [doi:10.7910/DVN/42MVDX](https://doi.org/10.7910/DVN/42MVDX) (v10.1) | CC0 1.0 | 0.5 MB | header check; SHA-256 recorded |
| County Presidential Returns 2000-2024 | MEDSL, [doi:10.7910/DVN/VOQCHQ](https://doi.org/10.7910/DVN/VOQCHQ) (v20.0) | CC0 1.0 | ~10 MB | header check; SHA-256 recorded |
| Congressional district boundaries, 1st-119th Congress | Lewis, DeVine, Pritcher and Martis, *Digital Boundary Definitions of United States Congressional Districts, 1789-2012* (and updates), <https://cdmaps.polisci.ucla.edu/>, GeoJSON from the GitHub repository above at the pinned commit | MIT | 14 KB-14 MB per state file (1.57 GB for all 907) | git blob SHA-1 against `lewis_index.json` |
| 119th Congress districts | US Census Bureau, 2024 cartographic boundary files `cb_2024_us_cd119_{500k,5m,20m}.zip`, <https://www2.census.gov/geo/tiger/GENZ2024/shp/> | Public domain | 7.0 MB / ~1.5 MB / 0.4 MB | zip must contain .shp/.shx/.dbf |
| Campaign finance | FEC OpenFEC API, <https://api.open.fec.gov/developers/> (needs an api.data.gov key) | US-government work, public domain; FEC rules restrict using individual contributor names and addresses for soliciting contributions or commercial purposes (52 U.S.C. § 30111(a)(4)). This worker returns only per-state aggregates of contributions, not contributor names. | per query | n/a |

The MEDSL data is CC0 and could legally be bundled, but Harvard Dataverse puts the House and
county files behind a guestbook (name, email, institution, position). The worker never submits
an identity on its own: pass your own answers as ATTACH options, or download the file in a
browser and save it at the path `cache_info()` shows. The Senate and President files have no
guestbook and download directly.

Geometry: both boundary sources publish NAD83 (EPSG:4269) longitude/latitude. The worker
passes the coordinates through unchanged and labels the column `GEOMETRY('OGC:CRS84')`; NAD83 and
WGS84 differ by about a metre in the conterminous US, below the files' generalisation.

## Test fixtures (`test/fixtures/`, used only by the offline tests)

Recorded on 2026-11-15 with fewer than 30 polite requests (User-Agent
`vgi-us-elections/0.1 (+https://query.farm)`, at most 1 request/s); FEC responses were recorded
with the public `DEMO_KEY`. See `test/fixtures/README.md` for what each file is. The House and
county result files are **synthetic** (placeholder candidates in the real file layout), because
the real files are guestbook-gated. Everything else is a real, trimmed upstream response.
