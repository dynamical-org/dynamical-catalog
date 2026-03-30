"""Slow integration tests that hit real network endpoints.

Run with: pytest -m slow
"""

import pytest
import xarray as xr

import dynamical

pytestmark = pytest.mark.slow


@pytest.fixture(autouse=True)
def fresh_catalog():
    """Ensure each test starts with a fresh catalog fetch."""
    dynamical.clear_cache()
    yield
    dynamical.clear_cache()


class TestStacCatalog:
    def test_catalog_loads(self):
        datasets = dynamical.list()
        assert len(datasets) > 0
        assert "noaa-gfs-forecast" in datasets

    def test_all_datasets_have_zarr_url(self):
        for dataset_id in dynamical.list():
            entry = getattr(dynamical.catalog, dataset_id.replace("-", "_"))
            assert entry.zarr_url, f"{dataset_id} missing zarr_url"

    def test_catalog_entry_metadata(self):
        entry = dynamical.catalog.noaa_gfs_forecast
        assert entry.id == "noaa-gfs-forecast"
        assert entry.name
        assert entry.description


class TestOpenZarr:
    def test_open_gfs_forecast(self):
        ds = dynamical.open("noaa-gfs-forecast")
        assert isinstance(ds, xr.Dataset)
        assert len(ds.data_vars) > 0

    def test_open_gfs_analysis(self):
        ds = dynamical.open("noaa-gfs-analysis")
        assert isinstance(ds, xr.Dataset)
        assert len(ds.data_vars) > 0

    def test_all_datasets_open(self):
        """Verify every dataset in the catalog can be opened without error."""
        for dataset_id in dynamical.list():
            ds = dynamical.open(dataset_id)
            assert isinstance(ds, xr.Dataset), f"{dataset_id} did not return a Dataset"
            assert len(ds.data_vars) > 0, f"{dataset_id} has no data variables"


class TestOpenIcechunk:
    def test_open_icechunk_gfs_forecast(self):
        icechunk = pytest.importorskip("icechunk")  # noqa: F841
        ds = dynamical.open("noaa-gfs-forecast", engine="icechunk")
        assert isinstance(ds, xr.Dataset)
        assert len(ds.data_vars) > 0
