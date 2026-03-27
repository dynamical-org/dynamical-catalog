from unittest.mock import patch

import pytest

from dynamical._catalog import Catalog
from dynamical._dataset_entry import DatasetEntry

SAMPLE_DATASETS = {
    "noaa-gfs-forecast": {
        "id": "noaa-gfs-forecast",
        "name": "NOAA GFS forecast",
        "description": "Weather forecasts from GFS.",
        "status": "live",
        "zarr_url": "https://data.dynamical.org/noaa/gfs/forecast/latest.zarr",
        "icechunk": {
            "bucket": "dynamical-noaa-gfs",
            "prefix": "noaa-gfs-forecast/v0.2.7.icechunk/",
            "region": "us-west-2",
        },
    },
    "noaa-gfs-analysis": {
        "id": "noaa-gfs-analysis",
        "name": "NOAA GFS analysis",
        "description": "Weather analysis from GFS.",
        "status": "live",
        "zarr_url": "https://data.dynamical.org/noaa/gfs/analysis/latest.zarr",
        "icechunk": {
            "bucket": "dynamical-noaa-gfs",
            "prefix": "noaa-gfs-analysis/v0.1.0.icechunk/",
            "region": "us-west-2",
        },
    },
    "noaa-gefs-forecast-35-day": {
        "id": "noaa-gefs-forecast-35-day",
        "name": "NOAA GEFS forecast, 35 day",
        "description": "Ensemble forecasts from GEFS.",
        "status": "live",
        "zarr_url": "https://data.dynamical.org/noaa/gefs/forecast-35-day/latest.zarr",
        "icechunk": None,
    },
}


@pytest.fixture
def catalog():
    return Catalog(lambda: SAMPLE_DATASETS)


class TestCatalog:
    def test_attribute_access(self, catalog):
        entry = catalog.noaa_gfs_forecast
        assert isinstance(entry, DatasetEntry)
        assert entry.id == "noaa-gfs-forecast"

    def test_attribute_access_returns_same_instance(self, catalog):
        a = catalog.noaa_gfs_forecast
        b = catalog.noaa_gfs_forecast
        assert a is b

    def test_unknown_dataset_raises(self, catalog):
        with pytest.raises(AttributeError, match="No dataset named"):
            catalog.nonexistent_dataset

    def test_private_attr_raises_normally(self, catalog):
        with pytest.raises(AttributeError):
            catalog._private

    def test_dir_includes_datasets(self, catalog):
        names = dir(catalog)
        assert "noaa_gfs_forecast" in names
        assert "noaa_gfs_analysis" in names
        assert "noaa_gefs_forecast_35_day" in names

    def test_repr(self, catalog):
        r = repr(catalog)
        assert "Catalog(" in r
        assert "noaa_gfs_forecast" in r


class TestDatasetEntry:
    def test_properties(self):
        entry = DatasetEntry(SAMPLE_DATASETS["noaa-gfs-forecast"])
        assert entry.id == "noaa-gfs-forecast"
        assert entry.name == "NOAA GFS forecast"
        assert "dynamical.org" in entry.zarr_url
        assert entry.status == "live"
        assert entry.icechunk_config is not None
        assert entry.icechunk_config["bucket"] == "dynamical-noaa-gfs"

    def test_repr(self):
        entry = DatasetEntry(SAMPLE_DATASETS["noaa-gfs-forecast"])
        assert "noaa-gfs-forecast" in repr(entry)

    def test_no_icechunk_config(self):
        data = {
            "id": "test",
            "name": "Test",
            "description": "",
            "zarr_url": "",
            "status": "live",
        }
        entry = DatasetEntry(data)
        assert entry.icechunk_config is None

    @patch("dynamical._open._open_dataset")
    def test_open_delegates_to_open_dataset(self, mock_open):
        entry = DatasetEntry(SAMPLE_DATASETS["noaa-gfs-forecast"])
        entry.open(engine="zarr", chunks={"time": 10})
        mock_open.assert_called_once_with(
            SAMPLE_DATASETS["noaa-gfs-forecast"], engine="zarr", chunks={"time": 10}
        )

    @patch("dynamical._open._get_store")
    def test_get_store_delegates(self, mock_get_store):
        entry = DatasetEntry(SAMPLE_DATASETS["noaa-gfs-forecast"])
        entry.get_store(engine="icechunk")
        mock_get_store.assert_called_once_with(
            SAMPLE_DATASETS["noaa-gfs-forecast"], engine="icechunk"
        )


class TestTopLevelAPI:
    @patch("dynamical._stac._datasets", SAMPLE_DATASETS)
    @patch("dynamical._open._open_dataset")
    def test_open_normalizes_underscores(self, mock_open):
        import dynamical
        dynamical.open("noaa_gfs_forecast")
        mock_open.assert_called_once_with(
            SAMPLE_DATASETS["noaa-gfs-forecast"], engine="zarr"
        )

    @patch("dynamical._stac._datasets", SAMPLE_DATASETS)
    @patch("dynamical._open._get_store")
    def test_get_store_normalizes_underscores(self, mock_get_store):
        import dynamical
        dynamical.get_store("noaa_gfs_forecast")
        mock_get_store.assert_called_once_with(
            SAMPLE_DATASETS["noaa-gfs-forecast"], engine="zarr"
        )

    @patch("dynamical._stac._datasets", SAMPLE_DATASETS)
    def test_open_unknown_raises_valueerror(self):
        import dynamical
        with pytest.raises(ValueError, match="Unknown dataset"):
            dynamical.open("nonexistent")

    @patch("dynamical._stac._datasets", SAMPLE_DATASETS)
    def test_list_returns_sorted_ids(self):
        import dynamical
        ids = dynamical.list()
        assert isinstance(ids, list)
        assert ids == sorted(ids)
        assert "noaa-gfs-forecast" in ids

    def test_catalog_is_catalog_instance(self):
        import dynamical
        assert isinstance(dynamical.catalog, Catalog)
