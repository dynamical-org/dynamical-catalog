"""Fetch and parse the dynamical.org STAC catalog."""

from __future__ import annotations

import concurrent.futures
import http.client
import json
import time
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urljoin, urlparse

from dynamical_catalog.exceptions import (
    CatalogFetchError,
    InvalidCatalogError,
)

STAC_CATALOG_URL = "https://stac.dynamical.org/catalog.json"

_TIMEOUT_SECONDS = 10
_MAX_ATTEMPTS = 3
_RETRY_BACKOFF_SECONDS = 1.0
_datasets: dict[str, dict[str, Any]] | None = None
_identifier: str | None = None


def set_identifier(identifier: str | None) -> None:
    """Set the identifier sent in the User-Agent header.

    Passing ``""`` or ``None`` disables identification. Empty strings are
    normalized to ``None`` so reads of ``_identifier`` are predictable.
    """
    global _identifier
    _identifier = identifier or None


def _user_agent() -> str:
    from dynamical_catalog import __version__

    ua = f"dynamical-catalog/{__version__}"
    if _identifier:
        ua += f" ({_identifier})"
    return ua


# Mid-stream / connection-level errors that can leak past urllib.error.URLError
# when the failure happens after urlopen() returns. Retrying is worth it because
# the next attempt opens a fresh connection.
_RETRIABLE_TRANSIENT_ERRORS = (
    urllib.error.URLError,
    TimeoutError,
    http.client.RemoteDisconnected,
    http.client.IncompleteRead,
)


def _fetch_json(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": _user_agent()})
    last_error: Exception | None = None
    for attempt in range(_MAX_ATTEMPTS):
        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS) as resp:
                body = resp.read()
        except urllib.error.HTTPError as e:
            # 4xx (except 429 Too Many Requests) won't change between attempts;
            # fail fast.
            if 400 <= e.code < 500 and e.code != 429:
                raise CatalogFetchError(
                    f"Failed to fetch dynamical.org STAC catalog from {url}: "
                    f"HTTP {e.code} {e.reason}",
                    urls=(url,),
                    attempts=attempt + 1,
                ) from e
            last_error = e
            if attempt < _MAX_ATTEMPTS - 1:
                time.sleep(_RETRY_BACKOFF_SECONDS)
            continue
        except _RETRIABLE_TRANSIENT_ERRORS as e:
            last_error = e
            if attempt < _MAX_ATTEMPTS - 1:
                time.sleep(_RETRY_BACKOFF_SECONDS)
            continue
        try:
            return json.loads(body)
        except json.JSONDecodeError as e:
            # Malformed JSON won't change between attempts; fail fast.
            raise CatalogFetchError(
                f"Failed to fetch dynamical.org STAC catalog from {url}: "
                f"response was not valid JSON: {e}",
                urls=(url,),
                attempts=attempt + 1,
            ) from e
    raise CatalogFetchError(
        f"Failed to fetch dynamical.org STAC catalog from {url}: {last_error}",
        urls=(url,),
        attempts=_MAX_ATTEMPTS,
    ) from last_error


def _parse_icechunk_asset(collection_id: str, asset: dict[str, Any]) -> dict[str, str]:
    href = asset["href"]
    parsed = urlparse(href)
    if parsed.scheme != "s3":
        raise InvalidCatalogError(
            f"STAC Collection {collection_id} icechunk asset href scheme is not "
            f"s3: {href!r}"
        )
    if not parsed.netloc:
        raise InvalidCatalogError(
            f"STAC Collection {collection_id} icechunk asset href is missing "
            f"a bucket: {href!r}"
        )
    if not parsed.path.lstrip("/"):
        raise InvalidCatalogError(
            f"STAC Collection {collection_id} icechunk asset href is missing "
            f"a prefix: {href!r}"
        )
    storage_options = asset.get("xarray:storage_options", {})
    client_kwargs = storage_options.get("client_kwargs", {})
    region = client_kwargs.get("region_name")
    if not region:
        raise InvalidCatalogError(
            f"STAC Collection {collection_id} icechunk asset is missing "
            f"xarray:storage_options.client_kwargs.region_name"
        )
    return {
        "bucket": parsed.netloc,
        "prefix": parsed.path.lstrip("/"),
        "region": region,
    }


def _parse_virtual_chunk_containers(
    collection_id: str, asset: dict[str, Any]
) -> list[str]:
    """Parse the allowed virtual-chunk container URL prefixes.

    Only anonymous S3 access is supported — a public catalog must not
    advertise static credentials.
    """
    containers = asset.get("icechunk:virtual_chunk_containers", [])
    if containers is None:
        containers = []
    if not isinstance(containers, list):
        raise InvalidCatalogError(
            f"STAC Collection {collection_id} icechunk:virtual_chunk_containers "
            f"must be a list, got {type(containers).__name__}: {containers!r}"
        )
    prefixes: list[str] = []
    for entry in containers:
        prefix = entry.get("url_prefix")
        if not isinstance(prefix, str) or not prefix.startswith("s3://"):
            raise InvalidCatalogError(
                f"STAC Collection {collection_id} virtual chunk container "
                f"url_prefix must be an s3:// string: {prefix!r}"
            )
        credentials = entry.get("credentials") or {}
        if credentials.get("type") != "s3" or not credentials.get("anonymous"):
            raise InvalidCatalogError(
                f"STAC Collection {collection_id} virtual chunk container "
                f"{prefix!r} must use {{type: 's3', anonymous: true}} credentials"
            )
        prefixes.append(prefix)
    return prefixes


def _parse_collection(collection: dict[str, Any]) -> dict[str, Any]:
    """Extract the dataset config we need from a STAC Collection."""
    if "id" not in collection:
        raise InvalidCatalogError("STAC Collection response is missing 'id'")
    collection_id = collection["id"]
    if "assets" not in collection:
        raise InvalidCatalogError(
            f"STAC Collection {collection_id} is missing 'assets'"
        )
    assets = collection["assets"]
    icechunk_asset = assets.get("icechunk")
    if icechunk_asset is None:
        raise InvalidCatalogError(
            f"STAC Collection {collection_id} is missing an 'icechunk' asset"
        )

    return {
        "id": collection_id,
        "name": collection.get("title", collection_id),
        "description": collection.get("description", ""),
        "icechunk": _parse_icechunk_asset(collection_id, icechunk_asset),
        "virtual_chunk_containers": _parse_virtual_chunk_containers(
            collection_id, icechunk_asset
        ),
    }


def load_catalog() -> dict[str, dict[str, Any]]:
    """Fetch the STAC catalog and all child collections.

    Results are cached in-process after the first call.
    Child collections are fetched in parallel for faster startup.
    """
    global _datasets
    if _datasets is not None:
        return _datasets

    catalog = _fetch_json(STAC_CATALOG_URL)
    if "links" not in catalog:
        raise InvalidCatalogError("STAC catalog response is missing 'links'")
    child_links = [link for link in catalog["links"] if link["rel"] == "child"]
    urls = [urljoin(STAC_CATALOG_URL, link["href"]) for link in child_links]

    collections: list[dict[str, Any]] = [{} for _ in urls]
    failures: list[tuple[str, Exception]] = []
    with concurrent.futures.ThreadPoolExecutor() as pool:
        future_to_index = {
            pool.submit(_fetch_json, url): i for i, url in enumerate(urls)
        }
        for future in concurrent.futures.as_completed(future_to_index):
            index = future_to_index[future]
            try:
                collections[index] = future.result()
            except Exception as e:
                failures.append((urls[index], e))

    if failures:
        failed_urls = tuple(url for url, _ in failures)
        first_error = failures[0][1]
        raise CatalogFetchError(
            f"Failed to fetch {len(failures)} STAC collection(s): {failed_urls}",
            urls=failed_urls,
            attempts=_MAX_ATTEMPTS,
        ) from first_error

    datasets: dict[str, dict[str, Any]] = {}
    seen_urls: dict[str, str] = {}
    for url, collection in zip(urls, collections, strict=True):
        parsed = _parse_collection(collection)
        dataset_id = parsed["id"]
        if dataset_id in datasets:
            raise InvalidCatalogError(
                f"STAC catalog contains duplicate dataset id {dataset_id!r}: "
                f"{seen_urls[dataset_id]} and {url}"
            )
        datasets[dataset_id] = parsed
        seen_urls[dataset_id] = url

    _datasets = datasets
    return _datasets


def clear_cache() -> None:
    """Clear the cached catalog data, forcing a fresh fetch on next access."""
    global _datasets
    _datasets = None
