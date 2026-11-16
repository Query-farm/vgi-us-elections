# Changelog

## 0.1.0 (unreleased)

- MEDSL House, Senate, President and county-presidential result tables from Harvard Dataverse,
  with year/state/district pushdown and derived vote_share / congress.
- Census apportionment 1910-2020 and a states table (bundled).
- `districts(congress, state := )` for the 1st-119th Congress (Lewis et al.) and
  `current_districts()` (Census 2024 cartographic boundaries) as `GEOMETRY('OGC:CRS84')`.
- OpenFEC candidates, candidate totals, committees and contributions by state
  (`us_elections_fec` secret).
- `normalize_district()`, `congress_for_year()`, `cache_info()`.

Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
Template: vgi-starter-python v1.3.1
