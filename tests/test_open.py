from unittest.mock import MagicMock, patch

import icechunk
import pytest
import xarray as xr
import zarr

from dynamical_catalog._open import _get_store, _open_dataset
from dynamical_catalog.exceptions import (
    DatasetOpenError,
    DynamicalCatalogError,
)


class TestGetStoreMocked:
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
        mock_icechunk.Repository.open.assert_called_once_with(
            mock_icechunk.s3_storage.return_value,
            authorize_virtual_chunk_access=None,
        )
        mock_repo = mock_icechunk.Repository.open.return_value
        assert store is mock_repo.readonly_session.return_value.store

    @patch("dynamical_catalog._open.icechunk")
    def test_authorizes_virtual_chunk_containers(self, mock_icechunk):
        data = {
            "id": "test",
            "icechunk": {
                "bucket": "dynamical-test",
                "prefix": "test/v0.1.0.icechunk/",
                "region": "us-west-2",
            },
            "virtual_chunk_containers": [
                "s3://noaa-gfs-bdp-pds",
                "s3://some-other-bucket",
            ],
        }
        anon_cred = mock_icechunk.s3_anonymous_credentials.return_value

        _get_store(data)

        mock_icechunk.containers_credentials.assert_called_once_with(
            {
                "s3://noaa-gfs-bdp-pds": anon_cred,
                "s3://some-other-bucket": anon_cred,
            }
        )
        mock_icechunk.Repository.open.assert_called_once_with(
            mock_icechunk.s3_storage.return_value,
            authorize_virtual_chunk_access=mock_icechunk.containers_credentials.return_value,
        )

    @patch("dynamical_catalog._open.icechunk")
    def test_virtual_chunk_containers_none_skips_authorization(self, mock_icechunk):
        # An explicit None (vs missing key) takes the same code path as []
        # via the `or []` fallback — no authorize block, no containers_credentials call.
        data = {
            "id": "test",
            "icechunk": {"bucket": "b", "prefix": "p/", "region": "us-west-2"},
            "virtual_chunk_containers": None,
        }

        _get_store(data)

        mock_icechunk.containers_credentials.assert_not_called()
        mock_icechunk.Repository.open.assert_called_once_with(
            mock_icechunk.s3_storage.return_value,
            authorize_virtual_chunk_access=None,
        )

    @patch("dynamical_catalog._open.icechunk")
    def test_virtual_chunk_containers_empty_list_skips_authorization(
        self, mock_icechunk
    ):
        # Empty list is falsy under `if prefixes`, so authorize stays None.
        data = {
            "id": "test",
            "icechunk": {"bucket": "b", "prefix": "p/", "region": "us-west-2"},
            "virtual_chunk_containers": [],
        }

        _get_store(data)

        mock_icechunk.containers_credentials.assert_not_called()
        mock_icechunk.Repository.open.assert_called_once_with(
            mock_icechunk.s3_storage.return_value,
            authorize_virtual_chunk_access=None,
        )


class TestGetStoreReal:
    """Build a real icechunk repo on local disk and exercise _get_store end-to-end.

    Catches drift in the real icechunk API that the fully-mocked tests above
    would silently miss. No network: icechunk.s3_storage is patched to return
    a local_filesystem_storage instead.
    """

    @pytest.fixture
    def local_repo_path(self, tmp_path):
        path = str(tmp_path / "repo")
        storage = icechunk.local_filesystem_storage(path)
        repo = icechunk.Repository.create(storage)
        session = repo.writable_session("main")
        root = zarr.create_group(store=session.store)
        root.create_array(
            name="values",
            shape=(4,),
            chunks=(4,),
            dtype="int32",
            compressors=None,
            dimension_names=("i",),
        )
        session.commit("seed")
        return path

    def test_get_store_yields_a_readable_zarr_store(self, local_repo_path, mocker):
        mocker.patch(
            "dynamical_catalog._open.icechunk.s3_storage",
            side_effect=lambda **_: icechunk.local_filesystem_storage(local_repo_path),
        )
        data = {
            "id": "test",
            "icechunk": {"bucket": "b", "prefix": "p/", "region": "us-west-2"},
        }

        store = _get_store(data)

        # The store should be openable by zarr; the seeded array exists.
        group = zarr.open_group(store=store, mode="r")
        assert "values" in group

    def test_open_dataset_yields_xarray_dataset(self, local_repo_path, mocker):
        mocker.patch(
            "dynamical_catalog._open.icechunk.s3_storage",
            side_effect=lambda **_: icechunk.local_filesystem_storage(local_repo_path),
        )
        data = {
            "id": "test",
            "icechunk": {"bucket": "b", "prefix": "p/", "region": "us-west-2"},
        }

        ds = _open_dataset(data)
        assert isinstance(ds, xr.Dataset)
        assert "values" in ds.data_vars


class TestGetStoreExceptionWrapping:
    @patch("dynamical_catalog._open.icechunk")
    def test_icechunk_error_is_wrapped(self, mock_icechunk):
        # Real subclass so the except-clause in _get_store matches.
        mock_icechunk.IcechunkError = icechunk.IcechunkError
        mock_icechunk.Repository.open.side_effect = icechunk.IcechunkError(
            "bucket not found"
        )
        data = {
            "id": "test",
            "icechunk": {"bucket": "b", "prefix": "p/", "region": "us-west-2"},
        }
        with pytest.raises(DatasetOpenError, match="Failed to open icechunk"):
            _get_store(data)

    @patch("dynamical_catalog._open.icechunk")
    def test_wrapped_exception_chains_original(self, mock_icechunk):
        mock_icechunk.IcechunkError = icechunk.IcechunkError
        original = icechunk.IcechunkError("bucket not found")
        mock_icechunk.Repository.open.side_effect = original
        data = {
            "id": "test",
            "icechunk": {"bucket": "b", "prefix": "p/", "region": "us-west-2"},
        }
        with pytest.raises(DatasetOpenError) as excinfo:
            _get_store(data)
        assert excinfo.value.__cause__ is original
        assert isinstance(excinfo.value, DynamicalCatalogError)


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
        mock_xr.open_zarr.assert_called_once_with(mock_store, consolidated=False)
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
            mock_get_store.return_value, chunks={"time": 10}, consolidated=False
        )

    @patch("dynamical_catalog._open.xr")
    @patch("dynamical_catalog._open._get_store")
    def test_caller_can_override_consolidated(self, mock_get_store, mock_xr):
        data = {
            "id": "test",
            "icechunk": {"bucket": "b", "prefix": "p/", "region": "us-west-2"},
        }

        _open_dataset(data, consolidated=True)

        mock_xr.open_zarr.assert_called_once_with(
            mock_get_store.return_value, consolidated=True
        )
