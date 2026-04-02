"""Slow integration tests that hit real network endpoints.

Run with: pytest -m slow
"""

import pytest
import xarray as xr

import dynamical_catalog

pytestmark = pytest.mark.slow


@pytest.fixture(autouse=True)
def fresh_catalog():
    """Ensure each test starts with a fresh catalog fetch."""
    dynamical_catalog.clear_cache()
    yield
    dynamical_catalog.clear_cache()


class TestStacCatalog:
    def test_catalog_loads(self):
        datasets = dynamical_catalog.list()
        assert len(datasets) > 0
        assert "noaa-gfs-forecast" in datasets

    def test_all_datasets_have_zarr_url(self):
        for dataset_id in dynamical_catalog.list():
            entry = getattr(dynamical_catalog.catalog, dataset_id.replace("-", "_"))
            assert entry.zarr_url, f"{dataset_id} missing zarr_url"

    def test_catalog_entry_metadata(self):
        entry = dynamical_catalog.catalog.noaa_gfs_forecast
        assert entry.id == "noaa-gfs-forecast"
        assert entry.name
        assert entry.description


class TestOpenZarr:
    def test_open_gfs_forecast(self):
        ds = dynamical_catalog.open("noaa-gfs-forecast")
        assert isinstance(ds, xr.Dataset)
        assert len(ds.data_vars) > 0

    def test_open_gfs_analysis(self):
        ds = dynamical_catalog.open("noaa-gfs-analysis")
        assert isinstance(ds, xr.Dataset)
        assert len(ds.data_vars) > 0

    def test_all_datasets_open(self):
        """Verify every dataset in the catalog can be opened without error."""
        for dataset_id in dynamical_catalog.list():
            ds = dynamical_catalog.open(dataset_id)
            assert isinstance(ds, xr.Dataset), f"{dataset_id} did not return a Dataset"
            assert len(ds.data_vars) > 0, f"{dataset_id} has no data variables"


class TestOpenAllEngines:
    def test_all_datasets_open_zarr(self):
        for dataset_id in dynamical_catalog.list():
            ds = dynamical_catalog.open(dataset_id, engine="zarr")
            assert isinstance(ds, xr.Dataset), f"{dataset_id} did not return a Dataset"
            assert len(ds.data_vars) > 0, f"{dataset_id} has no data variables"

    def test_all_icechunk_datasets_open_icechunk(self):
        for dataset_id in dynamical_catalog.list():
            entry = getattr(dynamical_catalog.catalog, dataset_id.replace("-", "_"))
            if entry.icechunk_config is None:
                continue
            ds = dynamical_catalog.open(dataset_id, engine="icechunk")
            assert isinstance(ds, xr.Dataset), f"{dataset_id} did not return a Dataset"
            assert len(ds.data_vars) > 0, f"{dataset_id} has no data variables"


class TestDatasetStructure:
    def test_all_datasets_have_spatial_coords(self):
        for dataset_id in dynamical_catalog.list():
            ds = dynamical_catalog.open(dataset_id)
            has_latlon = "latitude" in ds.dims and "longitude" in ds.dims
            has_xy = "x" in ds.dims and "y" in ds.dims
            assert has_latlon or has_xy, f"{dataset_id} missing spatial dims"
            assert "latitude" in ds.coords, f"{dataset_id} missing latitude coord"
            assert "longitude" in ds.coords, f"{dataset_id} missing longitude coord"

    def test_all_datasets_have_time_coord(self):
        for dataset_id in dynamical_catalog.list():
            ds = dynamical_catalog.open(dataset_id)
            has_time = "time" in ds.dims or "init_time" in ds.dims
            assert has_time, f"{dataset_id} missing time or init_time dim"
