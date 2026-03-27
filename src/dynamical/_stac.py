"""Fetch and parse the dynamical.org STAC catalog."""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urljoin

STAC_CATALOG_URL = "https://dynamical.org/stac/catalog.json"

_TIMEOUT_SECONDS = 30
_lock = threading.Lock()
_datasets: dict[str, dict[str, Any]] | None = None


def _fetch_json(url: str) -> Any:
    try:
        with urllib.request.urlopen(url, timeout=_TIMEOUT_SECONDS) as resp:
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
    """
    global _datasets
    if _datasets is not None:
        return _datasets

    with _lock:
        if _datasets is not None:
            return _datasets

        catalog = _fetch_json(STAC_CATALOG_URL)
        child_links = [link for link in catalog["links"] if link["rel"] == "child"]

        datasets: dict[str, dict[str, Any]] = {}
        for link in child_links:
            url = urljoin(STAC_CATALOG_URL, link["href"])
            collection = _fetch_json(url)
            parsed = _parse_collection(collection)
            datasets[parsed["id"]] = parsed

        _datasets = datasets
        return _datasets


def clear_cache() -> None:
    """Clear the cached catalog data, forcing a fresh fetch on next access."""
    global _datasets
    _datasets = None
