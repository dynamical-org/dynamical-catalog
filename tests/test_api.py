from unittest.mock import patch

import pytest

import dynamical_catalog
from tests.conftest import SAMPLE_DATASETS


class TestOpen:
    @patch("dynamical_catalog._stac._datasets", SAMPLE_DATASETS)
    @patch("dynamical_catalog._open._open_dataset")
    def test_open_by_hyphenated_id(self, mock_open):
        dynamical_catalog.open("noaa-gfs-forecast")
        mock_open.assert_called_once_with(SAMPLE_DATASETS["noaa-gfs-forecast"])

    @patch("dynamical_catalog._stac._datasets", SAMPLE_DATASETS)
    @patch("dynamical_catalog._open._open_dataset")
    def test_open_normalizes_underscores(self, mock_open):
        dynamical_catalog.open("noaa_gfs_forecast")
        mock_open.assert_called_once_with(SAMPLE_DATASETS["noaa-gfs-forecast"])

    @patch("dynamical_catalog._stac._datasets", SAMPLE_DATASETS)
    @patch("dynamical_catalog._open._open_dataset")
    def test_open_passes_kwargs(self, mock_open):
        dynamical_catalog.open("noaa-gfs-forecast", chunks={"time": 1})
        mock_open.assert_called_once_with(
            SAMPLE_DATASETS["noaa-gfs-forecast"], chunks={"time": 1}
        )

    @patch("dynamical_catalog._stac._datasets", SAMPLE_DATASETS)
    def test_open_unknown_raises_valueerror(self):
        with pytest.raises(ValueError, match="Unknown dataset"):
            dynamical_catalog.open("nonexistent")


class TestGetStore:
    @patch("dynamical_catalog._stac._datasets", SAMPLE_DATASETS)
    @patch("dynamical_catalog._open._get_store")
    def test_get_store_by_hyphenated_id(self, mock_get_store):
        dynamical_catalog.get_store("noaa-gfs-forecast")
        mock_get_store.assert_called_once_with(SAMPLE_DATASETS["noaa-gfs-forecast"])

    @patch("dynamical_catalog._stac._datasets", SAMPLE_DATASETS)
    @patch("dynamical_catalog._open._get_store")
    def test_get_store_normalizes_underscores(self, mock_get_store):
        dynamical_catalog.get_store("noaa_gfs_forecast")
        mock_get_store.assert_called_once_with(SAMPLE_DATASETS["noaa-gfs-forecast"])


class TestList:
    @patch("dynamical_catalog._stac._datasets", SAMPLE_DATASETS)
    def test_list_returns_sorted_ids(self):
        ids = dynamical_catalog.list()
        assert isinstance(ids, list)
        assert ids == sorted(ids)
        assert "noaa-gfs-forecast" in ids


class TestIdentify:
    def test_identify_sets_identifier(self):
        import dynamical_catalog._stac as stac_mod

        old_id = stac_mod._identifier
        try:
            dynamical_catalog.identify("marshall@dynamical.org")
            assert stac_mod._identifier == "marshall@dynamical.org"
        finally:
            stac_mod._identifier = old_id
