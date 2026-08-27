from unittest.mock import MagicMock, patch

import icechunk
import pytest
import xarray as xr
import zarr

from dynamical_catalog._open import (
    _build_storage,
    _container_credentials,
    _get_repository,
    _get_store,
    _open_dataset,
)
from dynamical_catalog.exceptions import (
    DatasetOpenError,
    DynamicalCatalogError,
)


def _with_containers(containers):
    """An S3 repo config carrying the given virtual chunk containers.

    The repository half is incidental in container-credential tests; container
    type is independent of repository storage type.
    """
    return {
        "id": "test",
        "icechunk": {
            "type": "s3",
            "bucket": "b",
            "prefix": "p/",
            "region": "us-west-2",
        },
        "virtual_chunk_containers": containers,
    }


class TestGetStoreMocked:
    @patch("dynamical_catalog._open.icechunk")
    def test_returns_icechunk_store(self, mock_icechunk):
        data = {
            "id": "test",
            "icechunk": {
                "type": "s3",
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
                "type": "s3",
                "bucket": "dynamical-test",
                "prefix": "test/v0.1.0.icechunk/",
                "region": "us-west-2",
            },
            "virtual_chunk_containers": [
                {"url_prefix": "s3://noaa-gfs-bdp-pds", "type": "s3"},
                {"url_prefix": "s3://some-other-bucket", "type": "s3"},
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
            "icechunk": {
                "type": "s3",
                "bucket": "b",
                "prefix": "p/",
                "region": "us-west-2",
            },
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
            "icechunk": {
                "type": "s3",
                "bucket": "b",
                "prefix": "p/",
                "region": "us-west-2",
            },
            "virtual_chunk_containers": [],
        }

        _get_store(data)

        mock_icechunk.containers_credentials.assert_not_called()
        mock_icechunk.Repository.open.assert_called_once_with(
            mock_icechunk.s3_storage.return_value,
            authorize_virtual_chunk_access=None,
        )


class TestGetStoreHttp:
    """HTTPS-backed repositories, e.g. an R2 bucket on a custom domain."""

    @patch("dynamical_catalog._open.icechunk")
    def test_uses_http_storage(self, mock_icechunk):
        data = {
            "id": "test",
            "icechunk": {
                "type": "http",
                "base_url": "https://data.example.org/test/v0.1.0.icechunk",
            },
        }

        store = _get_store(data)

        mock_icechunk.http_storage.assert_called_once_with(
            base_url="https://data.example.org/test/v0.1.0.icechunk"
        )
        mock_icechunk.s3_storage.assert_not_called()
        mock_icechunk.Repository.open.assert_called_once_with(
            mock_icechunk.http_storage.return_value,
            authorize_virtual_chunk_access=None,
        )
        mock_repo = mock_icechunk.Repository.open.return_value
        assert store is mock_repo.readonly_session.return_value.store

    @patch("dynamical_catalog._open.icechunk")
    def test_authorizes_http_virtual_chunk_containers(self, mock_icechunk):
        data = {
            "id": "test",
            "icechunk": {
                "type": "http",
                "base_url": "https://data.example.org/test/v0.1.0.icechunk",
            },
            "virtual_chunk_containers": [
                {"url_prefix": "https://chunks.example.org/data/", "type": "http"},
            ],
        }
        http_cred = mock_icechunk.Credentials.HttpAccess.return_value

        _get_store(data)

        mock_icechunk.containers_credentials.assert_called_once_with(
            {"https://chunks.example.org/data/": http_cred}
        )

    @patch("dynamical_catalog._open.icechunk")
    def test_mixes_s3_and_http_containers(self, mock_icechunk):
        # An S3-hosted repo may reference HTTPS virtual chunks and vice versa;
        # container type is independent of repository storage type.
        data = {
            "id": "test",
            "icechunk": {
                "type": "s3",
                "bucket": "b",
                "prefix": "p/",
                "region": "us-west-2",
            },
            "virtual_chunk_containers": [
                {"url_prefix": "s3://noaa-gfs-bdp-pds", "type": "s3"},
                {"url_prefix": "https://chunks.example.org/data/", "type": "http"},
            ],
        }

        _get_store(data)

        mock_icechunk.containers_credentials.assert_called_once_with(
            {
                "s3://noaa-gfs-bdp-pds": (
                    mock_icechunk.s3_anonymous_credentials.return_value
                ),
                "https://chunks.example.org/data/": (
                    mock_icechunk.Credentials.HttpAccess.return_value
                ),
            }
        )

    @patch("dynamical_catalog._open.icechunk")
    def test_unsupported_storage_type_raises(self, mock_icechunk):
        # local_filesystem_storage is a real icechunk backend, but not one a
        # public catalog can hand out, so no config parses to it.
        data = {"id": "test", "icechunk": {"type": "file", "path": "/tmp/repo"}}
        with pytest.raises(ValueError, match="Unsupported icechunk storage type"):
            _get_store(data)

    @patch("dynamical_catalog._open.icechunk")
    def test_unsupported_container_type_raises(self, mock_icechunk):
        data = {
            "id": "test",
            "icechunk": {
                "type": "s3",
                "bucket": "b",
                "prefix": "p/",
                "region": "us-west-2",
            },
            "virtual_chunk_containers": [
                {"url_prefix": "file:///chunks/", "type": "file"}
            ],
        }
        with pytest.raises(
            ValueError, match="Unsupported virtual chunk container type"
        ):
            _get_store(data)


class TestGetStoreObjectStores:
    """The remaining icechunk backends that support anonymous reads."""

    @patch("dynamical_catalog._open.icechunk")
    def test_uses_gcs_storage(self, mock_icechunk):
        data = {
            "id": "test",
            "icechunk": {
                "type": "gcs",
                "bucket": "dynamical-test",
                "prefix": "test/v0.1.0.icechunk/",
            },
        }

        store = _get_store(data)

        mock_icechunk.gcs_storage.assert_called_once_with(
            bucket="dynamical-test",
            prefix="test/v0.1.0.icechunk/",
            anonymous=True,
        )
        mock_icechunk.Repository.open.assert_called_once_with(
            mock_icechunk.gcs_storage.return_value,
            authorize_virtual_chunk_access=None,
        )
        mock_repo = mock_icechunk.Repository.open.return_value
        assert store is mock_repo.readonly_session.return_value.store

    @patch("dynamical_catalog._open.icechunk")
    def test_uses_azure_storage(self, mock_icechunk):
        data = {
            "id": "test",
            "icechunk": {
                "type": "azure",
                "account": "dynamicalstorage",
                "container": "dynamical-test",
                "prefix": "test/v0.1.0.icechunk/",
            },
        }

        _get_store(data)

        mock_icechunk.azure_storage.assert_called_once_with(
            account="dynamicalstorage",
            container="dynamical-test",
            prefix="test/v0.1.0.icechunk/",
            anonymous=True,
        )
        mock_icechunk.Repository.open.assert_called_once_with(
            mock_icechunk.azure_storage.return_value,
            authorize_virtual_chunk_access=None,
        )

    @patch("dynamical_catalog._open.icechunk")
    def test_uses_tigris_storage(self, mock_icechunk):
        data = {
            "id": "test",
            "icechunk": {
                "type": "tigris",
                "bucket": "dynamical-test",
                "prefix": "test/v0.1.0.icechunk/",
                "region": "iad",
            },
        }

        _get_store(data)

        mock_icechunk.tigris_storage.assert_called_once_with(
            bucket="dynamical-test",
            prefix="test/v0.1.0.icechunk/",
            region="iad",
            anonymous=True,
        )

    @patch("dynamical_catalog._open.icechunk")
    def test_authorizes_gcs_container(self, mock_icechunk):
        _get_store(_with_containers([{"url_prefix": "gs://public/", "type": "gcs"}]))

        mock_icechunk.gcs_credentials.assert_called_once_with(anonymous=True)
        mock_icechunk.containers_credentials.assert_called_once_with(
            {"gs://public/": mock_icechunk.gcs_credentials.return_value}
        )

    @patch("dynamical_catalog._open.icechunk")
    def test_authorizes_azure_container(self, mock_icechunk):
        _get_store(_with_containers([{"url_prefix": "az://public/", "type": "azure"}]))

        mock_icechunk.containers_credentials.assert_called_once_with(
            {"az://public/": mock_icechunk.azure_anonymous_credentials.return_value}
        )

    @patch("dynamical_catalog._open.icechunk")
    def test_tigris_container_reuses_s3_anonymous_credentials(self, mock_icechunk):
        # icechunk treats tigris:// as part of the S3 family of stores, so the
        # anonymous S3 credential is the right one.
        _get_store(
            _with_containers([{"url_prefix": "tigris://public/", "type": "tigris"}])
        )

        mock_icechunk.containers_credentials.assert_called_once_with(
            {"tigris://public/": mock_icechunk.s3_anonymous_credentials.return_value}
        )


class TestGetRepository:
    @patch("dynamical_catalog._open.icechunk")
    def test_returns_opened_repository(self, mock_icechunk):
        # get_repository hands back the Repository itself (history intact), not a
        # tip store — that's the whole reason it exists alongside _get_store.
        data = {
            "id": "test",
            "icechunk": {
                "type": "s3",
                "bucket": "dynamical-test",
                "prefix": "test/v0.1.0.icechunk/",
                "region": "us-west-2",
            },
        }

        repo = _get_repository(data)

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
        assert repo is mock_icechunk.Repository.open.return_value
        mock_icechunk.Repository.open.return_value.readonly_session.assert_not_called()

    @patch("dynamical_catalog._open.icechunk")
    def test_authorizes_virtual_chunk_containers(self, mock_icechunk):
        data = {
            "id": "test",
            "icechunk": {
                "type": "s3",
                "bucket": "b",
                "prefix": "p/",
                "region": "us-west-2",
            },
            "virtual_chunk_containers": [
                {"url_prefix": "s3://noaa-hrrr-bdp-pds", "type": "s3"}
            ],
        }
        anon_cred = mock_icechunk.s3_anonymous_credentials.return_value

        _get_repository(data)

        mock_icechunk.containers_credentials.assert_called_once_with(
            {"s3://noaa-hrrr-bdp-pds": anon_cred}
        )
        mock_icechunk.Repository.open.assert_called_once_with(
            mock_icechunk.s3_storage.return_value,
            authorize_virtual_chunk_access=mock_icechunk.containers_credentials.return_value,
        )

    @patch("dynamical_catalog._open.icechunk")
    def test_icechunk_error_is_wrapped(self, mock_icechunk):
        mock_icechunk.IcechunkError = icechunk.IcechunkError
        mock_icechunk.Repository.open.side_effect = icechunk.IcechunkError("boom")
        data = {
            "id": "test",
            "icechunk": {
                "type": "s3",
                "bucket": "b",
                "prefix": "p/",
                "region": "us-west-2",
            },
        }
        with pytest.raises(DatasetOpenError, match="Failed to open icechunk") as exc:
            _get_repository(data)
        assert exc.value.dataset_id == "test"


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
            "icechunk": {
                "type": "s3",
                "bucket": "b",
                "prefix": "p/",
                "region": "us-west-2",
            },
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
            "icechunk": {
                "type": "s3",
                "bucket": "b",
                "prefix": "p/",
                "region": "us-west-2",
            },
        }

        ds = _open_dataset(data)
        assert isinstance(ds, xr.Dataset)
        assert "values" in ds.data_vars


class TestBuildStorageReal:
    """Hand every backend's parsed config to the real icechunk constructors.

    The mocked tests above assert the call shape but would pass even if
    icechunk rejected the arguments. Constructing a Storage touches no network,
    yet it does validate: a regionless tigris_storage, for instance, raises
    rather than deferring to open time.
    """

    @pytest.mark.parametrize(
        "config",
        [
            pytest.param(
                {"type": "tigris", "bucket": "b", "prefix": "p/", "region": "iad"},
                id="tigris",
            ),
            pytest.param(
                {"type": "http", "base_url": "https://example.org/repo.icechunk"},
                id="http",
            ),
        ],
    )
    def test_builds_a_storage(self, config):
        assert isinstance(_build_storage(config), icechunk.Storage)

    @pytest.mark.parametrize(
        "container",
        [
            {"url_prefix": "s3://b/", "type": "s3"},
            {"url_prefix": "gs://b/", "type": "gcs"},
            {"url_prefix": "az://c/", "type": "azure"},
            {"url_prefix": "tigris://b/", "type": "tigris"},
            {"url_prefix": "https://example.org/chunks/", "type": "http"},
        ],
    )
    def test_container_credentials_are_accepted_by_icechunk(self, container):
        # containers_credentials is what rejects a credential of the wrong
        # family for a url_prefix's scheme.
        authorized = icechunk.containers_credentials(
            {container["url_prefix"]: _container_credentials(container)}
        )
        assert set(authorized) == {container["url_prefix"]}


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
            "icechunk": {
                "type": "s3",
                "bucket": "b",
                "prefix": "p/",
                "region": "us-west-2",
            },
        }
        with pytest.raises(DatasetOpenError, match="Failed to open icechunk") as exc:
            _get_store(data)
        assert exc.value.dataset_id == "test"

    @patch("dynamical_catalog._open.icechunk")
    def test_readonly_session_error_is_wrapped(self, mock_icechunk):
        # An IcechunkError raised while opening the "main" session (not by
        # Repository.open) is still surfaced as DatasetOpenError.
        mock_icechunk.IcechunkError = icechunk.IcechunkError
        mock_icechunk.Repository.open.return_value.readonly_session.side_effect = (
            icechunk.IcechunkError("no main branch")
        )
        data = {
            "id": "test",
            "icechunk": {
                "type": "s3",
                "bucket": "b",
                "prefix": "p/",
                "region": "us-west-2",
            },
        }
        with pytest.raises(DatasetOpenError, match="Failed to open icechunk") as exc:
            _get_store(data)
        assert exc.value.dataset_id == "test"

    @patch("dynamical_catalog._open.icechunk")
    def test_wrapped_exception_chains_original(self, mock_icechunk):
        mock_icechunk.IcechunkError = icechunk.IcechunkError
        original = icechunk.IcechunkError("bucket not found")
        mock_icechunk.Repository.open.side_effect = original
        data = {
            "id": "test",
            "icechunk": {
                "type": "s3",
                "bucket": "b",
                "prefix": "p/",
                "region": "us-west-2",
            },
        }
        with pytest.raises(DatasetOpenError) as excinfo:
            _get_store(data)
        assert excinfo.value.__cause__ is original
        assert excinfo.value.dataset_id == "test"
        assert isinstance(excinfo.value, DynamicalCatalogError)


class TestOpenDataset:
    @patch("dynamical_catalog._open.xr")
    @patch("dynamical_catalog._open._get_store")
    def test_passes_store_to_open_zarr(self, mock_get_store, mock_xr):
        mock_store = MagicMock()
        mock_get_store.return_value = mock_store
        data = {
            "id": "test",
            "icechunk": {
                "type": "s3",
                "bucket": "b",
                "prefix": "p/",
                "region": "us-west-2",
            },
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
            "icechunk": {
                "type": "s3",
                "bucket": "b",
                "prefix": "p/",
                "region": "us-west-2",
            },
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
            "icechunk": {
                "type": "s3",
                "bucket": "b",
                "prefix": "p/",
                "region": "us-west-2",
            },
        }

        _open_dataset(data, consolidated=True)

        mock_xr.open_zarr.assert_called_once_with(
            mock_get_store.return_value, consolidated=True
        )
