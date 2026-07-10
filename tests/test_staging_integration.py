"""Slow integration tests against the staging STAC catalog.

Staging (stac-staging.dynamical.org) publishes datasets before they reach
production, including multi-group virtual datasets whose vertical profiles
live in ``pressure_level`` / ``model_level`` groups. Production integration
tests (test_integration.py) only reach the production catalog and only open
each dataset's root group, so the vertical-group path has no real-published
coverage there. These tests point the public API at staging via
``DYNAMICAL_STAC_CATALOG_URL`` and exercise that path end to end.

GRIB decode on read is covered deterministically by test_virtual_open.py;
these tests open lazily, matching test_integration.py.

Run with: pytest -m slow
"""

import icechunk
import pytest
import xarray as xr

import dynamical_catalog
from dynamical_catalog._stac import CATALOG_URL_ENV_VAR, STAGING_STAC_CATALOG_URL

pytestmark = pytest.mark.slow

# Multi-group virtual dataset published to staging; carries both vertical groups.
_GROUPED_DATASET = "noaa-hrrr-forecast-48-hour-spatial"
_VERTICAL_GROUPS = ("pressure_level", "model_level")


@pytest.fixture(autouse=True)
def staging_catalog(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(CATALOG_URL_ENV_VAR, STAGING_STAC_CATALOG_URL)
    dynamical_catalog.clear_cache()
    yield
    dynamical_catalog.clear_cache()


class TestStagingCatalog:
    def test_catalog_loads(self):
        assert len(dynamical_catalog.list()) > 0

    def test_all_datasets_open(self):
        for dataset_id in dynamical_catalog.list():
            ds = dynamical_catalog.open(dataset_id)
            assert isinstance(ds, xr.Dataset), f"{dataset_id} did not return a Dataset"
            assert len(ds.data_vars) > 0, f"{dataset_id} has no data variables"


class TestVerticalGroups:
    def test_grouped_dataset_present(self):
        assert _GROUPED_DATASET in dynamical_catalog.list()

    @pytest.mark.parametrize("group", _VERTICAL_GROUPS)
    def test_open_vertical_group(self, group: str):
        ds = dynamical_catalog.open(_GROUPED_DATASET, group=group)
        assert isinstance(ds, xr.Dataset)
        assert group in ds.dims, f"{group} group is missing its {group!r} dimension"
        assert len(ds.data_vars) > 0, f"{group} group has no data variables"

    # This dataset's chunks are virtual refs into a public source bucket, so a
    # read only resolves if the staging collection advertises the source via
    # icechunk:virtual_chunk_containers. That emission is added by dynamical-stac
    # PR #38; until staging is redeployed with it, the read raises. strict=True
    # flips this to a failure once the read succeeds, prompting removal of the
    # marker and confirming the fix reached staging.
    @pytest.mark.xfail(
        raises=icechunk.IcechunkError,
        strict=True,
        reason="staging STAC does not yet advertise icechunk:virtual_chunk_containers",
    )
    def test_read_vertical_group_chunk(self):
        ds = dynamical_catalog.open(_GROUPED_DATASET, group="pressure_level")
        da = ds["geopotential_height"]
        value = da.isel(dict.fromkeys(da.dims, 0)).load().item()
        assert isinstance(value, float)
