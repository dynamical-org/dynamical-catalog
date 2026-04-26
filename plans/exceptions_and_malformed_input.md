# Exceptions and malformed-input handling

## Goal

Define a small, library-owned exception hierarchy so callers have stable types
to `except` against, and make every place we raise (or leak) an exception use
it. The current code mixes `ValueError`, `RuntimeError`, `KeyError`, and lets
`json.JSONDecodeError` / `icechunk.IcechunkError` / `xarray` errors escape
unwrapped — that couples callers to our internals.

This is a **follow-up** plan. The PR landing alongside this document only adds
tests that pin current behavior; the source changes below come next.

## Proposed hierarchy

In a new `dynamical_catalog/exceptions.py`, re-exported from the package root:

```
DynamicalCatalogError(Exception)            # base; "anything we raised"
├── UnknownDatasetError(DynamicalCatalogError, ValueError)
│       # user-supplied dataset_id is not in the catalog
├── CatalogFetchError(DynamicalCatalogError)
│       # network / HTTP / JSON-decode failure pulling the STAC catalog
├── InvalidCatalogError(DynamicalCatalogError)
│       # catalog response was reachable but contents failed validation
└── DatasetOpenError(DynamicalCatalogError)
        # failure opening the icechunk repo, zarr store, or xarray dataset
```

Conventions:
- Always `raise NewError(...) from original` so `__cause__` keeps the underlying
  exception for debugging without making it part of the public contract.
- `UnknownDatasetError` multi-inherits from `ValueError` so existing
  `except ValueError` callers keep working in the release that introduces the
  hierarchy.
- `CatalogFetchError` carries `url: str` and `attempts: int` attributes for
  programmatic handling.

## Sites to wrap (typed raises)

| Site | Today | Plan |
|---|---|---|
| `__init__.py:69-70` | `ValueError("Unknown dataset ...")` | `UnknownDatasetError` |
| `_stac.py:47-49` | `RuntimeError("Failed to fetch ...")` | `CatalogFetchError(url=..., attempts=...)` |
| `_stac.py:55-58` | `ValueError("not an s3:// URL")` | `InvalidCatalogError` |
| `_stac.py:63-66` | `ValueError("missing region_name")` | `InvalidCatalogError` |
| `_stac.py:86-90` | `ValueError("url_prefix must be an s3:// string")` | `InvalidCatalogError` |
| `_stac.py:92-96` | `ValueError("must use anonymous credentials")` | `InvalidCatalogError` |
| `_stac.py:107-109` | `ValueError("missing 'icechunk' asset")` | `InvalidCatalogError` |

## Malformed-input gaps

Each row below is currently untested *and* leaks an implementation-layer
exception or has surprising behavior. The PR landing with this plan adds tests
that **pin current behavior** for every row. The follow-up exception PR
implements the "Planned" column.

### 1. Malformed JSON response

- **Where:** `_stac.py:42` — `json.loads(resp.read())`.
- **Today:** `json.JSONDecodeError` escapes the retry loop unwrapped; one bad
  byte from the server bypasses the retry policy entirely.
- **Planned:** wrap the `json.loads` call inside the retry loop, treat decode
  failures as a retriable error, raise `CatalogFetchError` after exhaustion.

### 2. Failing child collection fetch

- **Where:** `_stac.py:137` — `pool.map(_fetch_json, urls)`.
- **Today:** `pool.map` raises lazily during iteration in `load_catalog`. The
  `RuntimeError` from `_fetch_json` reaches the caller, but the message
  identifies only the failing URL, not which dataset's collection it was.
- **Planned:** drain via `as_completed`, collect failures, raise a single
  `CatalogFetchError` listing every collection URL that failed.

### 3. Catalog response missing `links` key

- **Where:** `_stac.py:133` — `catalog["links"]`.
- **Today:** raw `KeyError: 'links'`.
- **Planned:** `InvalidCatalogError("STAC catalog response is missing 'links'")`.
  Same treatment for missing `id` / `assets` in collections (`_stac.py:103-105`).

### 4. Empty catalog (no child links)

- **Where:** `_stac.py:133-134` — filter for `rel == "child"`.
- **Today:** `dynamical_catalog.list()` silently returns `[]`. No error.
- **Planned:** keep silent `[]` — empty is a valid catalog state. Document in
  the docstring.

### 5. Duplicate dataset IDs across collections

- **Where:** `_stac.py:140-142` — second collection silently overwrites the
  first in the `datasets` dict.
- **Today:** silent, last-write-wins.
- **Planned:** `InvalidCatalogError` listing the duplicated id and the URLs
  involved.

### 6. `s3://` href edge cases

- **Where:** `_stac.py:53-58` — `_parse_icechunk_asset`.
- **Today:**
  - `s3://bucket/` (empty prefix after `lstrip('/')`) → "is not an s3:// URL"
    (misleading: it *is* an s3:// URL, just one with no prefix).
  - `s3:///prefix/` (empty bucket) → same misleading message.
- **Planned:** split into specific `InvalidCatalogError` messages — "missing
  bucket" vs "missing prefix" vs "scheme is not s3".

### 7. `icechunk:virtual_chunk_containers` shape variants

- **Where:** `_stac.py:82` — `asset.get("icechunk:virtual_chunk_containers", [])`.
- **Today:**
  - Missing key → `[]` (handled).
  - `None` value → crashes on iteration with `TypeError`.
  - Non-list value (e.g. `{}`, `"s3://..."`) → crashes on iteration or
    `.get()` with a confusing message.
- **Planned:** treat `None` as empty; `InvalidCatalogError` on any non-list,
  non-`None` value.

### 8. `identify("")` and `identify(None)`

- **Where:** `_stac.py:22-24` (`set_identifier`) and `__init__.py:17-26`
  (`identify`); consumed at `_stac.py:31-33` (`_user_agent`).
- **Today:** `_user_agent()` does `if _identifier:`, which is falsy for both
  `""` and `None` — so the parenthesized identifier is already correctly
  omitted at runtime. **Only the type signatures and docstrings are wrong:**
  both functions are typed as `(identifier: str)` and don't document the
  disable behavior.
- **Planned:** widen signatures to `str | None`, document that empty string
  and `None` disable identification, normalize `""` to `None` on assignment so
  reads of `_identifier` are predictable.

### 9. `identify(non_string)`

- **Where:** same as #8.
- **Today:** silently coerced into the User-Agent via f-string interpolation
  (e.g. `identify(42)` → `"dynamical-catalog/x.y.z (42)"`).
- **Planned:** no enforcement — `identify` is typed as `str | None` and
  callers passing other types are misusing the API. Tests pin the current
  coercion only as a regression guard.

## Sites to wrap (leaky implementation exceptions)

These don't have a "current message" to pin — they leak the underlying
library's exception type unchanged. The pinning tests assert today's leaky
type explicitly so the follow-up PR has to confront each one.

| Site | Leaks today | Plan |
|---|---|---|
| `_open.py:28` (`Repository.open`) | `icechunk.IcechunkError` (e.g., bucket not found, virtual chunk auth) | `DatasetOpenError`, chained |
| `_open.py:36` (`xr.open_zarr`) | `xarray` / `zarr` errors at open time | `DatasetOpenError`, chained |
| `_open.py:36` (lazy reads) | `xarray` / `zarr` / `icechunk` errors when user accesses data | **out of scope** — wrapping lazy errors would require proxying the returned `Dataset`. Document as best-effort: `DatasetOpenError` covers eager open, not lazy reads. |

## Migration steps (follow-up PR)

1. Add `dynamical_catalog/exceptions.py` and re-export from `__init__.py`.
2. Replace each row in the "typed raises" table.
3. Add `try/except` boundaries inside `_fetch_json`, `_parse_collection`,
   `load_catalog`, and `_get_store` / `_open_dataset` to convert leaks.
4. Update tests to assert the new types (and `isinstance(err, DynamicalCatalogError)`).
5. Note the behavior change in the changelog — especially the `IcechunkError`
   wrap and the new `CatalogFetchError` covering `JSONDecodeError`.

## Out of scope

- Logging / telemetry on exceptions.
- Retry policy changes.
- Async API.
- Wrapping lazy zarr/xarray/icechunk errors raised during data access.
