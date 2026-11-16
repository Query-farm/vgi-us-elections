-- make live-check: a few queries against the real upstream services (not part of make check).
LOAD vgi;
ATTACH 'us_elections' (TYPE vgi, LOCATION '@WORKER@');
SELECT source, item, status FROM us_elections.main.cache_info() ORDER BY ALL;
SELECT year, count(*) AS rows FROM us_elections.main.senate_results WHERE year >= 2020 GROUP BY ALL ORDER BY year;
SELECT candidate, round(100 * vote_share, 1) AS pct FROM us_elections.main.president_results
WHERE year = 2024 AND state_po = 'DE' AND NOT writein ORDER BY pct DESC LIMIT 3;
SELECT congress, state_po, district FROM us_elections.main.districts(118, state := 'DE');
SELECT count(*) AS districts FROM us_elections.main.current_districts(resolution := '20m');
-- FEC (skipped with an error message when FEC_API_KEY is unset).
CREATE SECRET fec (TYPE us_elections_fec, API_KEY '@FEC_KEY@');
SELECT name, receipts FROM us_elections.main.fec_candidate_totals(2024, state := 'DE', office := 'S') ORDER BY receipts DESC LIMIT 3;
