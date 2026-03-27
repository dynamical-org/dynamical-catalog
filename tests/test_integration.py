"""Slow integration tests that hit real network endpoints.

Run with: pytest -m slow
"""

import pytest
import xarray as xr

pytestmark = pytest.mark.slow


class TestStacCatalogReachable:
    def test_stac_catalog_loads(self):
        import dynamical._stac as stac_mod
        from dynamical._stac import load_catalog

        stac_mod._datasets = None  # force fresh fetch
        datasets = load_catalog()
        assert len(datasets) > 0
        assert "noaa-gfs-forecast" in datasets
        stac_mod._datasets = None  # cleanup


class TestOpenZarr:
    def test_open_gfs_forecast(self):
        import dynamical

        ds = dynamical.open("noaa-gfs-forecast")
        assert isinstance(ds, xr.Dataset)
        assert len(ds.data_vars) > 0

    def test_open_gfs_analysis(self):
        import dynamical

        ds = dynamical.open("noaa-gfs-analysis")
        assert isinstance(ds, xr.Dataset)
        assert len(ds.data_vars) > 0


class TestOpenIcechunk:
    def test_open_icechunk_gfs_forecast(self):
        icechunk = pytest.importorskip("icechunk")  # noqa: F841
        import dynamical

        ds = dynamical.open("noaa-gfs-forecast", engine="icechunk")
        assert isinstance(ds, xr.Dataset)
        assert len(ds.data_vars) > 0
