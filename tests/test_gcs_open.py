"""Open a real icechunk repo on public GCS through dynamical_catalog.

``gs://dynamical-icechunk-gcs-demo`` (dynamical-org GCP project, ``allUsers``
objectViewer) holds a one-array repo whose single chunk is a virtual ref into
``demo-chunks/values.bin`` in the same bucket, so this covers both the ``gcs``
repository path and the ``gcs`` virtual chunk container path. STAC fetching is
patched so ``load_catalog`` returns a matching collection.

Run with: pytest -m slow
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from typing import Any
from unittest.mock import patch

import icechunk
import numpy as np
import pytest
import xarray as xr

import dynamical_catalog
import dynamical_catalog._stac as stac

pytestmark = pytest.mark.slow

_BUCKET = "dynamical-icechunk-gcs-demo"
_CONTAINER_PREFIX = f"gs://{_BUCKET}/demo-chunks/"
_VALUES = np.arange(8, dtype="int32")
_DATASET_ID = "gcs-demo"
_COLLECTION_URL = f"https://stac.dynamical.org/{_DATASET_ID}/collection.json"


@pytest.fixture(autouse=True)
def no_ambient_google_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> Iterator[None]:
    """Hide gcloud ADC and GOOGLE_* so anonymous access is what gets tested."""
    for key in list(os.environ):
        if key.startswith(("GOOGLE_", "CLOUDSDK_")):
            monkeypatch.delenv(key)
    monkeypatch.setenv("HOME", str(tmp_path))
    with patch.object(stac, "_datasets", None):
        yield


def _mock_stac_fetch(*, with_virtual_containers: bool) -> Callable[[str], Any]:
    catalog = {
        "type": "Catalog",
        "id": "dynamical-org-test",
        "stac_version": "1.0.0",
        "links": [
            {"rel": "self", "href": stac.STAC_CATALOG_URL},
            {"rel": "root", "href": stac.STAC_CATALOG_URL},
            {"rel": "child", "href": f"./{_DATASET_ID}/collection.json"},
        ],
    }
    icechunk_asset: dict[str, Any] = {
        "href": f"gs://{_BUCKET}/demo.icechunk/",
        "type": "application/x-icechunk",
    }
    if with_virtual_containers:
        icechunk_asset["icechunk:virtual_chunk_containers"] = [
            {
                "url_prefix": _CONTAINER_PREFIX,
                "credentials": {"type": "gcs", "anonymous": True},
            }
        ]
    collection = {
        "type": "Collection",
        "id": _DATASET_ID,
        "stac_version": "1.0.0",
        "title": "GCS demo",
        "description": "One array, one virtual chunk, all on public GCS.",
        "license": "CC-BY-4.0",
        "assets": {"icechunk": icechunk_asset},
        "links": [],
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2026-09-02T00:00:00Z", None]]},
        },
    }
    responses = {stac.STAC_CATALOG_URL: catalog, _COLLECTION_URL: collection}

    def fetch(url: str) -> Any:
        return responses[url]

    return fetch


class TestGcsOpen:
    def test_opens_and_reads_gcs_virtual_chunk(self) -> None:
        with patch.object(
            stac,
            "_fetch_json",
            side_effect=_mock_stac_fetch(with_virtual_containers=True),
        ):
            ds = dynamical_catalog.open(_DATASET_ID)
            assert isinstance(ds, xr.Dataset)
            np.testing.assert_array_equal(ds["values"].values, _VALUES)

    def test_missing_virtual_chunk_containers_blocks_read(self) -> None:
        with patch.object(
            stac,
            "_fetch_json",
            side_effect=_mock_stac_fetch(with_virtual_containers=False),
        ):
            ds = dynamical_catalog.open(_DATASET_ID)
            with pytest.raises(icechunk.IcechunkError, match="virtual chunk"):
                _ = ds["values"].values
