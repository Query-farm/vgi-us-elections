# Changelog

## 0.2.0 (2026-11-25)

- Speaks VGI protocol 3.4: require vgi-python `>=0.55.2,<0.56`; uv.lock re-locked. This release
  needs Haybarn 2.0.1 (or stock DuckDB 2.0 with `INSTALL vgi FROM query_farm`): Haybarn 2.0.0
  loads a 3.3 `vgi` extension that cannot attach it.
- SQL tests run on haybarn-unittest 2.0.1rc3 (CI and Makefile).
- New `serve` extra (`vgi-python[http,oauth]`); the Docker image installs it, so the image runs in
  Orchard hosted mode (`VGI_ORCHARD_HOSTED=1` with VGI_JWT_ISSUER, VGI_JWT_AUDIENCE,
  VGI_JWT_JWKS_URI and VGI_SIGNING_KEY). The Docker HEALTHCHECK uses `HEAD /health`, which
  stays anonymous in hosted mode.

## 0.1.1 (2026-11-16)

- Bump vgi-python to 0.52.2.
- Requires Python 3.14 (CI, Docker image and tooling moved to 3.14).

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
