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

### 1. Staleness check (the big one)
No test verifies data is **fresh**. A dataset could open fine but be days/weeks behind if a reformatter pipeline stalls. Add a test that reads the max `time` or `init_time` coordinate and asserts it's within a reasonable window (e.g., 5 days).

### 2. Icechunk coverage for all datasets
`test_all_datasets_open` uses the default engine (icechunk with zarr fallback). No test explicitly opens ALL datasets via both engines where applicable.

### 3. Coordinate/variable sanity
No tests verify expected dimensions exist. A schema change could silently break downstream users.

### 4. STAC URL resolution
`test_all_datasets_have_zarr_url` checks the URL string exists but doesn't verify it resolves (HTTP HEAD).

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
- [ ] Implement tests
- [ ] Tests pass against live data
