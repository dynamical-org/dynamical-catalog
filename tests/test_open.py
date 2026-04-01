from unittest.mock import MagicMock, patch

import pytest

from dynamical_catalog._open import _get_store, _open_dataset


class TestGetStoreZarr:
    @patch("dynamical_catalog._open.zarr.storage.FsspecStore.from_url")
    def test_returns_fsspec_store(self, mock_from_url):
        mock_from_url.return_value = MagicMock()
        data = {"id": "test", "zarr_url": "https://example.com/test.zarr"}

        store = _get_store(data, engine="zarr")

        mock_from_url.assert_called_once_with("https://example.com/test.zarr")
        assert store is mock_from_url.return_value


class TestGetStoreIcechunk:
    @patch("dynamical_catalog._open.icechunk")
    def test_returns_icechunk_store(self, mock_icechunk):
        data = {
            "id": "test",
            "zarr_url": "https://example.com/test.zarr",
            "icechunk": {
                "bucket": "dynamical-test",
                "prefix": "test/v0.1.0.icechunk/",
                "region": "us-west-2",
            },
        }

        store = _get_store(data, engine="icechunk")

        mock_icechunk.s3_storage.assert_called_once_with(
            bucket="dynamical-test",
            prefix="test/v0.1.0.icechunk/",
            region="us-west-2",
            anonymous=True,
        )
        mock_icechunk.Repository.open.assert_called_once()
        mock_repo = mock_icechunk.Repository.open.return_value
        assert store is mock_repo.readonly_session.return_value.store

    @patch("dynamical_catalog._open.icechunk")
    def test_no_icechunk_config_raises(self, _mock_icechunk):
        data = {"id": "test", "zarr_url": "https://example.com/test.zarr"}

        with pytest.raises(ValueError, match="does not have icechunk"):
            _get_store(data, engine="icechunk")


class TestGetStoreUnknownEngine:
    def test_raises_valueerror(self):
        data = {"id": "test", "zarr_url": "https://example.com/test.zarr"}

        with pytest.raises(ValueError, match="Unknown engine"):
            _get_store(data, engine="unknown")


class TestOpenDataset:
    @patch("dynamical_catalog._open.xr")
    @patch("dynamical_catalog._open._get_store")
    def test_passes_store_to_open_zarr(self, mock_get_store, mock_xr):
        mock_store = MagicMock()
        mock_get_store.return_value = mock_store
        data = {"id": "test", "zarr_url": "https://example.com/test.zarr"}

        result = _open_dataset(data, engine="zarr")

        mock_get_store.assert_called_once_with(data, engine="zarr")
        mock_xr.open_zarr.assert_called_once_with(mock_store)
        assert result is mock_xr.open_zarr.return_value

    @patch("dynamical_catalog._open.xr")
    @patch("dynamical_catalog._open._get_store")
    def test_passes_kwargs(self, mock_get_store, mock_xr):
        data = {"id": "test", "zarr_url": "https://example.com/test.zarr"}

        _open_dataset(data, engine="zarr", chunks={"time": 10})

        mock_xr.open_zarr.assert_called_once_with(
            mock_get_store.return_value, chunks={"time": 10}
        )
