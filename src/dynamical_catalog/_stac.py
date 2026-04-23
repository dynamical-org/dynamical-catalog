"""Fetch and parse the dynamical.org STAC catalog."""

from __future__ import annotations

import concurrent.futures
import json
import time
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urljoin, urlparse

STAC_CATALOG_URL = "https://stac.dynamical.org/catalog.json"

_TIMEOUT_SECONDS = 10
_MAX_ATTEMPTS = 3
_RETRY_BACKOFF_SECONDS = 1.0
_datasets: dict[str, dict[str, Any]] | None = None
_identifier: str | None = None


def set_identifier(identifier: str) -> None:
    global _identifier
    _identifier = identifier


def _user_agent() -> str:
    from dynamical_catalog import __version__

    ua = f"dynamical-catalog/{__version__}"
    if _identifier:
        ua += f" ({_identifier})"
    return ua


def _fetch_json(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": _user_agent()})
    last_error: Exception | None = None
    for attempt in range(_MAX_ATTEMPTS):
        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS) as resp:
                return json.loads(resp.read())
        except urllib.error.URLError as e:
            last_error = e
            if attempt < _MAX_ATTEMPTS - 1:
                time.sleep(_RETRY_BACKOFF_SECONDS)
    raise RuntimeError(
        f"Failed to fetch dynamical.org STAC catalog from {url}: {last_error}"
    ) from last_error


def _parse_icechunk_asset(collection_id: str, asset: dict[str, Any]) -> dict[str, str]:
    parsed = urlparse(asset["href"])
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError(
            f"STAC Collection {collection_id} icechunk asset href is not an s3:// URL "
            f"with bucket and prefix: {asset['href']!r}"
        )
    storage_options = asset.get("xarray:storage_options", {})
    client_kwargs = storage_options.get("client_kwargs", {})
    region = client_kwargs.get("region_name")
    if not region:
        raise ValueError(
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
    prefixes: list[str] = []
    for entry in containers:
        prefix = entry.get("url_prefix")
        if not isinstance(prefix, str) or not prefix.startswith("s3://"):
            raise ValueError(
                f"STAC Collection {collection_id} virtual chunk container "
                f"url_prefix must be an s3:// string: {prefix!r}"
            )
        credentials = entry.get("credentials") or {}
        if credentials.get("type") != "s3" or not credentials.get("anonymous"):
            raise ValueError(
                f"STAC Collection {collection_id} virtual chunk container "
                f"{prefix!r} must use {{type: 's3', anonymous: true}} credentials"
            )
        prefixes.append(prefix)
    return prefixes


def _parse_collection(collection: dict[str, Any]) -> dict[str, Any]:
    """Extract the dataset config we need from a STAC Collection."""
    collection_id = collection["id"]
    assets = collection.get("assets", {})
    icechunk_asset = assets.get("icechunk")
    if icechunk_asset is None:
        raise ValueError(
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
    child_links = [link for link in catalog["links"] if link["rel"] == "child"]
    urls = [urljoin(STAC_CATALOG_URL, link["href"]) for link in child_links]

    with concurrent.futures.ThreadPoolExecutor() as pool:
        collections = pool.map(_fetch_json, urls)

    datasets: dict[str, dict[str, Any]] = {}
    for collection in collections:
        parsed = _parse_collection(collection)
        datasets[parsed["id"]] = parsed

    _datasets = datasets
    return _datasets


def clear_cache() -> None:
    """Clear the cached catalog data, forcing a fresh fetch on next access."""
    global _datasets
    _datasets = None
