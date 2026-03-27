from unittest.mock import MagicMock, patch

import pytest

from dynamical._open import _open_dataset


class TestOpenZarr:
    @patch("dynamical._open.xr")
    def test_opens_with_zarr_url(self, mock_xr):
        mock_xr.open_zarr.return_value = MagicMock()
        data = {"id": "test", "zarr_url": "https://example.com/test.zarr"}

        result = _open_dataset(data, engine="zarr")

        mock_xr.open_zarr.assert_called_once_with("https://example.com/test.zarr")
        assert result is mock_xr.open_zarr.return_value

    @patch("dynamical._open.xr")
    def test_passes_kwargs(self, mock_xr):
        data = {"id": "test", "zarr_url": "https://example.com/test.zarr"}

        _open_dataset(data, engine="zarr", chunks={"time": 10})

        mock_xr.open_zarr.assert_called_once_with(
            "https://example.com/test.zarr", chunks={"time": 10}
        )


class TestOpenIcechunk:
    @patch("dynamical._open.xr")
    def test_opens_with_icechunk(self, mock_xr):
        mock_icechunk = MagicMock()
        data = {
            "id": "test",
            "zarr_url": "https://example.com/test.zarr",
            "icechunk": {
                "bucket": "dynamical-test",
                "prefix": "test/v0.1.0.icechunk/",
                "region": "us-west-2",
            },
        }

        with patch.dict("sys.modules", {"icechunk": mock_icechunk}):
            _open_dataset(data, engine="icechunk")

        mock_icechunk.s3_storage.assert_called_once_with(
            bucket="dynamical-test",
            prefix="test/v0.1.0.icechunk/",
            region="us-west-2",
            anonymous=True,
        )
        mock_icechunk.Repository.open.assert_called_once()

    @patch("dynamical._open.xr")
    def test_icechunk_defaults_chunks_none(self, mock_xr):
        mock_icechunk = MagicMock()
        data = {
            "id": "test",
            "zarr_url": "https://example.com/test.zarr",
            "icechunk": {
                "bucket": "dynamical-test",
                "prefix": "test/v0.1.0.icechunk/",
                "region": "us-west-2",
            },
        }

        with patch.dict("sys.modules", {"icechunk": mock_icechunk}):
            _open_dataset(data, engine="icechunk")

        call_kwargs = mock_xr.open_zarr.call_args[1]
        assert call_kwargs["chunks"] is None

    @patch("dynamical._open.xr")
    def test_icechunk_kwargs_override(self, mock_xr):
        mock_icechunk = MagicMock()
        data = {
            "id": "test",
            "zarr_url": "https://example.com/test.zarr",
            "icechunk": {
                "bucket": "dynamical-test",
                "prefix": "test/v0.1.0.icechunk/",
                "region": "us-west-2",
            },
        }

        with patch.dict("sys.modules", {"icechunk": mock_icechunk}):
            _open_dataset(data, engine="icechunk", chunks={"time": 10})

        call_kwargs = mock_xr.open_zarr.call_args[1]
        assert call_kwargs["chunks"] == {"time": 10}

    def test_no_icechunk_config_raises(self):
        mock_icechunk = MagicMock()
        data = {"id": "test", "zarr_url": "https://example.com/test.zarr"}

        with patch.dict("sys.modules", {"icechunk": mock_icechunk}):
            with pytest.raises(ValueError, match="does not have icechunk configuration"):
                _open_dataset(data, engine="icechunk")


class TestUnknownEngine:
    def test_raises_valueerror(self):
        data = {"id": "test", "zarr_url": "https://example.com/test.zarr"}

        with pytest.raises(ValueError, match="Unknown engine"):
            _open_dataset(data, engine="unknown")
