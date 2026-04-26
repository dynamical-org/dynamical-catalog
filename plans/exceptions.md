# Wrapping Raised Exceptions Behind a Public Set

## Context

Today the library raises a mix of stdlib exceptions (`ValueError`, `RuntimeError`, `KeyError`) and lets some implementation-layer exceptions escape unwrapped (`json.JSONDecodeError`, `icechunk.IcechunkError`, `xarray`/`zarr` errors). This couples callers to our internals: a user who wants to `except` "the dataset wasn't found" or "the network failed" has no stable type to target, and renaming/replacing an internal dependency could break their `except` clauses.

This plan lists every site that raises (or leaks) an exception and proposes a small public exception hierarchy that hides the implementation while preserving useful detail via `__cause__`.

## Proposed hierarchy

Defined in a new `dynamical_catalog/exceptions.py`, re-exported from the package root:

```
DynamicalCatalogError(Exception)            # base; users can catch this for "anything we raised"
├── UnknownDatasetError(DynamicalCatalogError, ValueError)
│       # user-supplied dataset_id is not in the catalog
├── CatalogFetchError(DynamicalCatalogError)
│       # network / HTTP / JSON-decode failure pulling STAC catalog or a child collection
├── InvalidCatalogError(DynamicalCatalogError)
│       # catalog response was reachable but its contents failed our validation
└── DatasetOpenError(DynamicalCatalogError)
        # failure opening the icechunk repo, zarr store, or xarray dataset
```

Notes:
- `UnknownDatasetError` multi-inherits from `ValueError` so existing `except ValueError` callers don't break in the same release we introduce the hierarchy.
- All wrappers should `raise NewError(...) from original` so `__cause__` preserves the underlying exception for debugging without making it part of the public contract.
- Base class lives in `exceptions.py` to avoid a circular import with `_stac.py` / `_open.py`.

## Sites to wrap

### `src/dynamical_catalog/__init__.py`

| Line | Currently raises | Proposed |
|---|---|---|
| 69-70 | `ValueError("Unknown dataset ...")` | `UnknownDatasetError` (still `isinstance(..., ValueError)` via multi-inheritance) |

### `src/dynamical_catalog/_stac.py`

| Line | Currently raises | Proposed |
|---|---|---|
| 47-49 | `RuntimeError("Failed to fetch ...")` after retries | `CatalogFetchError`, chained from the last `URLError` |
| 42 (implicit) | `json.JSONDecodeError` if the response isn't JSON | wrap inside `_fetch_json` as `CatalogFetchError` |
| 55-58 | `ValueError("is not an s3:// URL")` | `InvalidCatalogError` |
| 63-66 | `ValueError("missing ... region_name")` | `InvalidCatalogError` |
| 86-90 | `ValueError("url_prefix must be an s3:// string")` | `InvalidCatalogError` |
| 92-96 | `ValueError("must use {type: 's3', anonymous: true} credentials")` | `InvalidCatalogError` |
| 107-109 | `ValueError("missing an 'icechunk' asset")` | `InvalidCatalogError` |
| 103, 133 (implicit) | `KeyError` on `collection["id"]`, `catalog["links"]`, missing required STAC fields | `InvalidCatalogError` (catch `KeyError` once at the parse boundary) |

### `src/dynamical_catalog/_open.py`

| Line | Currently raises | Proposed |
|---|---|---|
| 28 (implicit) | `icechunk.IcechunkError` from `Repository.open` (e.g., virtual chunk auth, bucket not found) | wrap as `DatasetOpenError` |
| 36 (implicit) | `xarray` / `zarr` errors from `xr.open_zarr` | wrap as `DatasetOpenError` |

`IcechunkError` is currently observable in tests (see `tests/test_virtual_open.py:168`) — wrapping it is a behavior change worth calling out in the changelog.

## Open questions

1. **Public re-export surface.** Re-export only `DynamicalCatalogError` plus the four leaf classes from the package root, or re-export everything via `from dynamical_catalog.exceptions import *`? Preference: explicit `__all__` listing the five names.
2. **Lazy errors from `xr.open_zarr`.** Many zarr/xarray failures don't surface until the user accesses data (e.g., `ds["var"].values`). Wrapping at `_open_dataset` only catches eager errors. Decide whether `DatasetOpenError` is best-effort (eager only) or whether we need a lazy wrapper around the returned `Dataset` (probably out of scope — eager only).
3. **`UnknownDatasetError` ↔ `ValueError`.** Keep multi-inheritance permanently as a stable contract, or treat it as a transitional hack we drop in a future major version?
4. **Retry-aware errors.** Should `CatalogFetchError` carry structured fields (`attempts`, `url`, `final_status`) for programmatic handling, or keep it message-only? Preference: add `url` and `attempts` attributes — cheap, useful.
5. **Where to wrap `KeyError`.** Wrap at each access site (verbose) or wrap once in `_parse_collection` / `load_catalog` with a `try: ... except KeyError as e: raise InvalidCatalogError(...) from e` boundary? Preference: boundary wrap at the parse functions.

## Migration steps (when undertaken)

1. Add `dynamical_catalog/exceptions.py` with the hierarchy.
2. Re-export from `__init__.py`; document in README.
3. Replace `raise ValueError/RuntimeError` sites in order of the table above.
4. Add `try/except` boundaries around `urlopen` (`json.JSONDecodeError`), `_parse_*` (`KeyError`), and `_open` (`icechunk.IcechunkError`, zarr/xarray errors).
5. Update tests in `test_stac.py`, `test_open.py`, `test_api.py`, `test_virtual_open.py` to assert the new types — keep one test per leaf class that also asserts `isinstance(err, DynamicalCatalogError)` and that `err.__cause__` is the original.
6. Note the behavior change in the changelog (especially `IcechunkError` no longer leaking).

## Out of scope

- Logging / telemetry on exceptions.
- Retry policy changes.
- Async API.
