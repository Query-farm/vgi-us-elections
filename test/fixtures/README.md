# Test fixtures

Recorded or hand-written upstream responses served by `test/mock_api/server.py`, so the
tests run offline. None of these are used at runtime.

| Path | What | Provenance |
|---|---|---|
| `dataverse/datasets/*.json` | Harvard Dataverse dataset API responses, trimmed to the fields the worker reads | recorded 2026-11-15 |
| `dataverse/files/13887039.csv` | MEDSL Senate 1976-2024, rows for 2018+ in DE/GA/AZ/WY/NE | real subset (CC0) |
| `dataverse/files/13887042.csv` | MEDSL President 1976-2024, 2016/2020/2024 in DE/GA/AZ/WY/NE | real subset (CC0) |
| `dataverse/files/13592823.csv` | MEDSL House file layout (header from the dataset's DDI metadata) | **synthetic rows**: the real file sits behind a Dataverse guestbook, so candidates are placeholders |
| `dataverse/files/13573089.csv` | MEDSL county-presidential layout (header from DDI metadata) | **synthetic rows**, same reason |
| `lewis/GeoJson/*.geojson` | Lewis et al. district files at commit 3c663a8 | real (MIT) |
| `census/cb_2024_us_cd119_20m.zip` | Census 119th Congress cartographic boundaries, 1:20m | real (public domain) |
| `fec/*.json` | OpenFEC responses for Delaware 2024, recorded with the public DEMO_KEY; designated-agent contact fields removed; page 2 of committees/totals trimmed | recorded 2026-11-15 |
