from unittest.mock import MagicMock, patch

from dynamical_catalog._open import _get_store, _open_dataset


class TestGetStore:
    @patch("dynamical_catalog._open.icechunk")
    def test_returns_icechunk_store(self, mock_icechunk):
        data = {
            "id": "test",
            "icechunk": {
                "bucket": "dynamical-test",
                "prefix": "test/v0.1.0.icechunk/",
                "region": "us-west-2",
            },
        }

        store = _get_store(data)

        mock_icechunk.s3_storage.assert_called_once_with(
            bucket="dynamical-test",
            prefix="test/v0.1.0.icechunk/",
            region="us-west-2",
            anonymous=True,
        )
        mock_icechunk.Repository.open.assert_called_once()
        mock_repo = mock_icechunk.Repository.open.return_value
        assert store is mock_repo.readonly_session.return_value.store


class TestOpenDataset:
    @patch("dynamical_catalog._open.xr")
    @patch("dynamical_catalog._open._get_store")
    def test_passes_store_to_open_zarr(self, mock_get_store, mock_xr):
        mock_store = MagicMock()
        mock_get_store.return_value = mock_store
        data = {
            "id": "test",
            "icechunk": {"bucket": "b", "prefix": "p/", "region": "us-west-2"},
        }

        result = _open_dataset(data)

        mock_get_store.assert_called_once_with(data)
        mock_xr.open_zarr.assert_called_once_with(mock_store)
        assert result is mock_xr.open_zarr.return_value

    @patch("dynamical_catalog._open.xr")
    @patch("dynamical_catalog._open._get_store")
    def test_passes_kwargs(self, mock_get_store, mock_xr):
        data = {
            "id": "test",
            "icechunk": {"bucket": "b", "prefix": "p/", "region": "us-west-2"},
        }

        _open_dataset(data, chunks={"time": 10})

        mock_xr.open_zarr.assert_called_once_with(
            mock_get_store.return_value, chunks={"time": 10}
        )
