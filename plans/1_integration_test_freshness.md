# Integration Testing: STAC + dynamical_catalog

## Context

The catalog library fetches a live STAC catalog from `dynamical.org/stac/catalog.json`, parses collections into dataset configs, and opens them via Zarr or Icechunk. We need integration tests that prove the data is **accessible** and **not stale** — catching pipeline failures where a dataset is technically openable but hasn't been updated in days.

## What exists today

### Unit tests (mocked, CI on every push/PR — `test.yml`)
- `test_stac.py` — STAC fetch/parse/cache/user-agent, all mocked
- `test_open.py` — store creation + `open_zarr`, all mocked
- `test_catalog.py` — `Catalog`, `DatasetEntry`, top-level API, all mocked

### Integration tests (real network, daily cron — `integration.yml`)
- `test_integration.py` — `@pytest.mark.slow`, runs `pytest -m slow`
  - **TestStacCatalog**: catalog loads, all datasets have `zarr_url`, metadata present
  - **TestOpenZarr**: opens GFS forecast + analysis, opens ALL datasets (`test_all_datasets_open`)
  - **TestOpenIcechunk**: opens GFS forecast via icechunk (single dataset only)

### What's already well-covered
- STAC catalog fetches and parses correctly
- Every dataset in the catalog can be opened as an xarray Dataset with `len(data_vars) > 0`
- Icechunk path works for at least one dataset

## Gaps to fill

### ~~1. Staleness check~~ — moved to dynamical.org repo
Per PR discussion: staleness should be checked by construction when building the STAC, not in this client library.

### 2. Icechunk coverage for all datasets
`test_all_datasets_open` uses the default engine (icechunk with zarr fallback). No test explicitly opens ALL datasets via both engines where applicable.

### 3. Coordinate/variable sanity
No tests verify expected dimensions exist. A schema change could silently break downstream users.

### ~~4. STAC URL resolution~~ — moved to dynamical.org repo
Per PR discussion: data accessibility checks belong in the STAC build process.

## Implementation plan

### File to modify: `tests/test_integration.py`

1. **Add `TestFreshness`** class:
   - For each dataset, open it (lazy), read max of `time` or `init_time` coord
   - Assert max time > `now - timedelta(days=5)`

2. **Add `TestOpenAllEngines`** class:
   - `test_all_datasets_open_zarr` — loop all datasets, `engine="zarr"`
   - `test_all_icechunk_datasets_open_icechunk` — loop datasets with icechunk config, `engine="icechunk"`

3. **Add `TestDatasetStructure`** class:
   - Assert every dataset has `time` or `init_time` dimension
   - Assert every dataset has `latitude` and `longitude` dimensions

4. **Add STAC URL HEAD check** to `TestStacCatalog`:
   - HEAD-request each dataset's `zarr_url` + `/zarr.json`, assert 200

## Verification
- `uv run pytest tests/test_integration.py -v -m slow`

## Status
- [x] Branch + draft PR created (PR #1)
- [x] Implement tests
- [x] Tests pass against live data (10/10 integration, 37/37 unit)

## Notes
- HRRR uses projected `x`/`y` dims (Lambert conformal) with 2D `latitude`/`longitude` coords, so spatial check accepts either pattern
- Staleness + STAC URL resolution deferred to dynamical.org repo per PR discussion
