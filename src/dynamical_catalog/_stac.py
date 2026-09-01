"""Fetch and parse the dynamical.org STAC catalog."""

from __future__ import annotations

import concurrent.futures
import http.client
import json
import os
import time
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import ParseResult, urljoin, urlparse

from dynamical_catalog.exceptions import (
    CatalogFetchError,
    InvalidCatalogError,
)

STAC_CATALOG_URL = "https://stac.dynamical.org/catalog.json"
STAGING_STAC_CATALOG_URL = "https://stac-staging.dynamical.org/catalog.json"
# Override the catalog URL to point at a non-production catalog (e.g. staging).
CATALOG_URL_ENV_VAR = "DYNAMICAL_STAC_CATALOG_URL"

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


def _catalog_url() -> str:
    return os.environ.get(CATALOG_URL_ENV_VAR) or STAC_CATALOG_URL


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
            # 4xx won't change between attempts and fails fast, except:
            # - 429 Too Many Requests, which is inherently transient
            # - 409 Conflict, which the CDN/object store backing stac.dynamical.org
            #   can return for a read racing an in-flight write
            if 400 <= e.code < 500 and e.code not in (429, 409):
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


# Asset href scheme -> storage type. Only backends icechunk can read anonymously.
_HREF_SCHEME_TO_STORAGE_TYPE = {
    "s3": "s3",
    "gs": "gcs",
    "gcs": "gcs",
    "az": "azure",
    "azure": "azure",
    "abfs": "azure",
    "tigris": "tigris",
    "https": "http",
}


def _parse_icechunk_asset(collection_id: str, asset: dict[str, Any]) -> dict[str, Any]:
    """Parse the icechunk asset href into storage config.

    There is no ``r2://`` scheme: R2's S3 endpoint rejects unsigned requests,
    so public R2 buckets are read over ``https://`` instead.
    """
    href = asset["href"]
    parsed = urlparse(href)
    storage_type = _HREF_SCHEME_TO_STORAGE_TYPE.get(parsed.scheme)
    if storage_type is None:
        raise InvalidCatalogError(
            f"STAC Collection {collection_id} icechunk asset href scheme is not "
            f"one of {sorted(_HREF_SCHEME_TO_STORAGE_TYPE)}: {href!r}"
        )
    return _HREF_PARSERS[storage_type](collection_id, asset, href, parsed)


def _storage_options(asset: dict[str, Any]) -> dict[str, Any]:
    return asset.get("xarray:storage_options") or {}


def _region(asset: dict[str, Any]) -> Any:
    return (_storage_options(asset).get("client_kwargs") or {}).get("region_name")


def _split_bucket_prefix(
    collection_id: str,
    href: str,
    parsed: ParseResult,
    *,
    bucket_label: str = "bucket",
) -> tuple[str, str]:
    """Split a ``scheme://bucket/prefix`` href into its bucket and prefix."""
    if not parsed.netloc:
        raise InvalidCatalogError(
            f"STAC Collection {collection_id} icechunk asset href is missing "
            f"a {bucket_label}: {href!r}"
        )
    prefix = parsed.path.lstrip("/")
    if not prefix:
        raise InvalidCatalogError(
            f"STAC Collection {collection_id} icechunk asset href is missing "
            f"a prefix: {href!r}"
        )
    return parsed.netloc, prefix


def _required_region(collection_id: str, asset: dict[str, Any]) -> str:
    region = _region(asset)
    if not region:
        raise InvalidCatalogError(
            f"STAC Collection {collection_id} icechunk asset is missing "
            f"xarray:storage_options.client_kwargs.region_name"
        )
    return region


def _parse_http_href(
    collection_id: str, asset: dict[str, Any], href: str, parsed: ParseResult
) -> dict[str, Any]:
    if not parsed.netloc:
        raise InvalidCatalogError(
            f"STAC Collection {collection_id} icechunk asset href is missing "
            f"a host: {href!r}"
        )
    if not parsed.path.strip("/"):
        raise InvalidCatalogError(
            f"STAC Collection {collection_id} icechunk asset href is missing "
            f"a prefix: {href!r}"
        )
    # icechunk concatenates keys onto base_url; a trailing slash doubles the
    # separator and surfaces as a misleading "repository doesn't exist".
    return {"type": "http", "base_url": href.rstrip("/")}


def _parse_s3_href(
    collection_id: str, asset: dict[str, Any], href: str, parsed: ParseResult
) -> dict[str, Any]:
    bucket, prefix = _split_bucket_prefix(collection_id, href, parsed)
    return {
        "type": "s3",
        "bucket": bucket,
        "prefix": prefix,
        "region": _required_region(collection_id, asset),
    }


def _parse_gcs_href(
    collection_id: str, asset: dict[str, Any], href: str, parsed: ParseResult
) -> dict[str, Any]:
    bucket, prefix = _split_bucket_prefix(collection_id, href, parsed)
    return {"type": "gcs", "bucket": bucket, "prefix": prefix}


def _parse_azure_href(
    collection_id: str, asset: dict[str, Any], href: str, parsed: ParseResult
) -> dict[str, Any]:
    container, prefix = _split_bucket_prefix(
        collection_id, href, parsed, bucket_label="container"
    )
    # The href carries only the container, so the account comes from options.
    account = _storage_options(asset).get("account_name")
    if not account:
        raise InvalidCatalogError(
            f"STAC Collection {collection_id} icechunk asset is missing "
            f"xarray:storage_options.account_name"
        )
    return {
        "type": "azure",
        "account": account,
        "container": container,
        "prefix": prefix,
    }


def _parse_tigris_href(
    collection_id: str, asset: dict[str, Any], href: str, parsed: ParseResult
) -> dict[str, Any]:
    bucket, prefix = _split_bucket_prefix(collection_id, href, parsed)
    return {
        "type": "tigris",
        "bucket": bucket,
        "prefix": prefix,
        # icechunk rejects a Tigris store with no region.
        "region": _required_region(collection_id, asset),
    }


_HREF_PARSERS = {
    "s3": _parse_s3_href,
    "gcs": _parse_gcs_href,
    "azure": _parse_azure_href,
    "tigris": _parse_tigris_href,
    "http": _parse_http_href,
}


# Container credential type -> the url_prefix schemes icechunk pairs it with.
_CONTAINER_SCHEMES = {
    "s3": ("s3://",),
    "gcs": ("gs://", "gcs://"),
    "azure": ("az://", "azure://", "abfs://"),
    "tigris": ("tigris://",),
    "http": ("https://",),
}
# Types whose backend signs requests, so must opt into anonymous access.
_SIGNED_CONTAINER_TYPES = frozenset(_CONTAINER_SCHEMES) - {"http"}
_ALLOWED_CREDENTIAL_KEYS = {"type", "anonymous"}


def _parse_virtual_chunk_containers(
    collection_id: str, asset: dict[str, Any]
) -> list[dict[str, str]]:
    """Parse the allowed virtual-chunk containers.

    Returns ``{"url_prefix": ..., "type": ...}`` dicts. Only anonymous access
    is accepted — a public catalog must not advertise static credentials.
    """
    containers = asset.get("icechunk:virtual_chunk_containers", [])
    if containers is None:
        containers = []
    if not isinstance(containers, list):
        raise InvalidCatalogError(
            f"STAC Collection {collection_id} icechunk:virtual_chunk_containers "
            f"must be a list, got {type(containers).__name__}: {containers!r}"
        )
    parsed: list[dict[str, str]] = []
    for entry in containers:
        prefix = entry.get("url_prefix")
        credentials = entry.get("credentials") or {}
        container_type = credentials.get("type")
        if not isinstance(container_type, str) or container_type not in (
            _CONTAINER_SCHEMES
        ):
            raise InvalidCatalogError(
                f"STAC Collection {collection_id} virtual chunk container "
                f"{prefix!r} credentials type must be one of "
                f"{sorted(_CONTAINER_SCHEMES)}: {container_type!r}"
            )
        schemes = _CONTAINER_SCHEMES[container_type]
        if not isinstance(prefix, str) or not prefix.startswith(schemes):
            raise InvalidCatalogError(
                f"STAC Collection {collection_id} virtual chunk container "
                f"url_prefix must be a {' or '.join(schemes)} string for "
                f"{container_type!r} credentials: {prefix!r}"
            )
        extra_keys = set(credentials) - _ALLOWED_CREDENTIAL_KEYS
        if extra_keys:
            raise InvalidCatalogError(
                f"STAC Collection {collection_id} virtual chunk container "
                f"{prefix!r} must not carry credential material; unexpected "
                f"keys: {sorted(extra_keys)}"
            )
        anonymous = credentials.get("anonymous")
        if container_type in _SIGNED_CONTAINER_TYPES and not anonymous:
            raise InvalidCatalogError(
                f"STAC Collection {collection_id} virtual chunk container "
                f"{prefix!r} must use {{type: {container_type!r}, "
                f"anonymous: true}} credentials"
            )
        if anonymous is False:
            raise InvalidCatalogError(
                f"STAC Collection {collection_id} virtual chunk container "
                f"{prefix!r} must be anonymous"
            )
        parsed.append({"url_prefix": prefix, "type": container_type})
    return parsed


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

    catalog_url = _catalog_url()
    catalog = _fetch_json(catalog_url)
    if "links" not in catalog:
        raise InvalidCatalogError("STAC catalog response is missing 'links'")
    child_links = [link for link in catalog["links"] if link["rel"] == "child"]
    urls = [urljoin(catalog_url, link["href"]) for link in child_links]

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
