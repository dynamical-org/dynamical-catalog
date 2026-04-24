"""Build a real virtual icechunk repo and open it through dynamical_catalog.

The repo is created on local disk, but holds one virtual chunk ref pointing at
real bytes in the public ``noaa-gefs-pds`` S3 bucket. We then:

- patch ``icechunk.s3_storage`` so ``_get_store`` resolves to the local repo
- patch STAC fetching so ``load_catalog`` returns a collection that matches

This exercises the full path that the catalog uses to serve virtual-chunk-backed
datasets, including the ``icechunk:virtual_chunk_containers`` authorization step.

Run with: pytest -m slow
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any
from unittest.mock import patch

import icechunk
import pytest
import xarray as xr
import zarr

import dynamical_catalog
import dynamical_catalog._stac as stac

pytestmark = pytest.mark.slow

# Offsets from s3://noaa-gefs-pds/gefs.20260401/00/atmos/pgrb2ap5/gec00.t00z.pgrb2a.0p50.f000.idx:
#   1:0:d=2026040100:HGT:10 mb:anl:ENS=low-res ctl
#   2:235395:d=2026040100:TMP:10 mb:anl:ENS=low-res ctl
# The first grib message spans bytes [0, 235395).
_GRIB_URL = (
    "s3://noaa-gefs-pds/gefs.20260401/00/atmos/pgrb2ap5/gec00.t00z.pgrb2a.0p50.f000"
)
_GRIB_OFFSET = 0
_GRIB_LENGTH = 235395
_CONTAINER_PREFIX = "s3://noaa-gefs-pds/"
_DATASET_ID = "gefs-virtual-test"
_COLLECTION_URL = f"https://stac.dynamical.org/{_DATASET_ID}/collection.json"


@pytest.fixture(scope="module")
def virtual_icechunk_repo(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Create a local icechunk repo with one virtual chunk pointing at real S3 bytes."""
    path = str(tmp_path_factory.mktemp("virtual_icechunk_repo"))
    storage = icechunk.local_filesystem_storage(path)
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            url_prefix=_CONTAINER_PREFIX,
            store=icechunk.s3_store(region="us-east-1", anonymous=True),
        )
    )
    authorize = icechunk.containers_credentials(
        {_CONTAINER_PREFIX: icechunk.s3_anonymous_credentials()}
    )
    repo = icechunk.Repository.create(
        storage, config=config, authorize_virtual_chunk_access=authorize
    )
    session = repo.writable_session("main")
    root = zarr.create_group(store=session.store)
    root.create_array(
        name="grib_bytes",
        shape=(_GRIB_LENGTH,),
        chunks=(_GRIB_LENGTH,),
        dtype="uint8",
        compressors=None,
        dimension_names=("byte",),
    )
    session.store.set_virtual_ref(
        "grib_bytes/c/0",
        _GRIB_URL,
        offset=_GRIB_OFFSET,
        length=_GRIB_LENGTH,
    )
    session.commit("add virtual ref to one grib message")
    return path


@pytest.fixture
def patched_s3_storage(virtual_icechunk_repo: str) -> Iterator[None]:
    """Redirect ``icechunk.s3_storage`` to the local virtual repo for this test."""

    def fake_s3_storage(**_kwargs: Any) -> icechunk.Storage:
        return icechunk.local_filesystem_storage(virtual_icechunk_repo)

    with (
        patch.object(stac, "_datasets", None),
        patch(
            "dynamical_catalog._open.icechunk.s3_storage", side_effect=fake_s3_storage
        ),
    ):
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
        "href": "s3://not-used/not-used/",
        "type": "application/x-icechunk",
        "xarray:storage_options": {
            "anon": True,
            "client_kwargs": {"region_name": "us-east-1"},
        },
    }
    if with_virtual_containers:
        icechunk_asset["icechunk:virtual_chunk_containers"] = [
            {
                "url_prefix": _CONTAINER_PREFIX,
                "credentials": {"type": "s3", "anonymous": True},
            }
        ]
    collection = {
        "type": "Collection",
        "id": _DATASET_ID,
        "stac_version": "1.0.0",
        "title": "GEFS virtual test",
        "description": "Test dataset backed by a single virtual grib chunk.",
        "license": "CC-BY-4.0",
        "assets": {"icechunk": icechunk_asset},
        "links": [],
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2026-04-01T00:00:00Z", None]]},
        },
    }
    responses = {stac.STAC_CATALOG_URL: catalog, _COLLECTION_URL: collection}

    def fetch(url: str) -> Any:
        return responses[url]

    return fetch


class TestVirtualOpen:
    def test_opens_and_reads_virtual_chunk(self, patched_s3_storage: None) -> None:
        with patch.object(
            stac,
            "_fetch_json",
            side_effect=_mock_stac_fetch(with_virtual_containers=True),
        ):
            ds = dynamical_catalog.open(_DATASET_ID)
            assert isinstance(ds, xr.Dataset)
            head = ds["grib_bytes"].values[:4]
            assert bytes(head.tolist()) == b"GRIB"

    def test_missing_virtual_chunk_containers_blocks_read(
        self, patched_s3_storage: None
    ) -> None:
        with patch.object(
            stac,
            "_fetch_json",
            side_effect=_mock_stac_fetch(with_virtual_containers=False),
        ):
            ds = dynamical_catalog.open(_DATASET_ID)
            with pytest.raises(icechunk.IcechunkError, match="virtual chunk"):
                _ = ds["grib_bytes"].values
