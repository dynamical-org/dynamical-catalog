"""Fetch and parse the dynamical.org STAC catalog."""

from __future__ import annotations

import concurrent.futures
import json
import threading
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urljoin

STAC_CATALOG_URL = "https://stac.dynamical.org/catalog.json"

_TIMEOUT_SECONDS = 30
_lock = threading.Lock()
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
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS) as resp:
            return json.loads(resp.read())
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"Failed to fetch dynamical.org STAC catalog from {url}: {e}"
        ) from e


def _parse_collection(collection: dict[str, Any]) -> dict[str, Any]:
    """Extract the dataset config we need from a STAC Collection."""
    assets = collection.get("assets", {})
    zarr_asset = assets.get("zarr")
    if zarr_asset is None:
        raise ValueError(
            f"STAC Collection {collection.get('id', '?')} is missing a 'zarr' asset"
        )

    result: dict[str, Any] = {
        "id": collection["id"],
        "name": collection.get("title", collection["id"]),
        "description": collection.get("description", ""),
        "status": "live",
        "zarr_url": zarr_asset["href"],
    }

    icechunk_asset = assets.get("icechunk")
    if icechunk_asset is not None:
        storage = icechunk_asset.get("icechunk:storage", {})
        result["icechunk"] = {
            "bucket": storage["bucket"],
            "prefix": storage["prefix"],
            "region": storage["region"],
        }

    return result


def load_catalog() -> dict[str, dict[str, Any]]:
    """Fetch the STAC catalog and all child collections.

    Results are cached in-process after the first call.
    Child collections are fetched in parallel for faster startup.
    """
    global _datasets
    if _datasets is not None:
        return _datasets

    with _lock:
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
