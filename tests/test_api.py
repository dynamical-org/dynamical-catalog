import warnings

import pytest

import dynamical_catalog
import dynamical_catalog._stac as stac
from dynamical_catalog.exceptions import (
    DynamicalCatalogError,
    InvalidCatalogError,
    UnknownDatasetError,
)

_FUTURE_URL = "https://stac.dynamical.org/future-dataset/collection.json"


def _add_future_dataset(served_catalog):
    """Add a collection on a storage backend this client doesn't know."""
    root = served_catalog.responses["https://stac.dynamical.org/catalog.json"]
    served_catalog.responses["https://stac.dynamical.org/catalog.json"] = {
        **root,
        "links": [*root["links"], {"rel": "child", "href": _FUTURE_URL}],
    }
    served_catalog.responses[_FUTURE_URL] = {
        "id": "future-dataset",
        "assets": {"icechunk": {"href": "ftp://bucket/prefix/"}},
    }


class TestOpen:
    def test_open_by_dataset_id(self, populated_catalog, mocker):
        mock_open = mocker.patch("dynamical_catalog._open._open_dataset")
        dynamical_catalog.open("noaa-gfs-forecast")
        mock_open.assert_called_once_with(populated_catalog["noaa-gfs-forecast"])

    def test_open_underscore_id_resolves_with_deprecation_warning(
        self, populated_catalog, mocker
    ):
        mock_open = mocker.patch("dynamical_catalog._open._open_dataset")
        with pytest.warns(DeprecationWarning, match="Underscores in dataset ids"):
            dynamical_catalog.open("noaa_gfs_forecast")
        mock_open.assert_called_once_with(populated_catalog["noaa-gfs-forecast"])

    def test_open_underscore_unknown_id_raises_without_deprecation_warning(
        self, populated_catalog
    ):
        # An underscore id that doesn't resolve should surface as
        # UnknownDatasetError, not a misleading deprecation notice.
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            with pytest.raises(UnknownDatasetError):
                dynamical_catalog.open("nonexistent_dataset")

    def test_open_passes_kwargs(self, populated_catalog, mocker):
        mock_open = mocker.patch("dynamical_catalog._open._open_dataset")
        dynamical_catalog.open("noaa-gfs-forecast", chunks={"time": 1})
        mock_open.assert_called_once_with(
            populated_catalog["noaa-gfs-forecast"], chunks={"time": 1}
        )

    def test_open_unknown_raises_unknown_dataset_error(self, populated_catalog):
        with pytest.raises(UnknownDatasetError, match="Unknown dataset"):
            dynamical_catalog.open("nonexistent")

    def test_open_unknown_is_value_error_for_compat(self, populated_catalog):
        # UnknownDatasetError multi-inherits from ValueError so callers that
        # catch ValueError keep working.
        with pytest.raises(ValueError, match="Unknown dataset"):
            dynamical_catalog.open("nonexistent")

    def test_open_unknown_is_dynamical_catalog_error(self, populated_catalog):
        with pytest.raises(DynamicalCatalogError):
            dynamical_catalog.open("nonexistent")

    def test_open_unknown_lists_available_sorted(self, populated_catalog):
        with pytest.raises(UnknownDatasetError) as excinfo:
            dynamical_catalog.open("nonexistent")
        message = str(excinfo.value)
        # Available datasets are listed sorted in the error message.
        sorted_ids = sorted(populated_catalog.keys())
        positions = [message.find(ds_id) for ds_id in sorted_ids]
        assert all(p >= 0 for p in positions)
        assert positions == sorted(positions)

    def test_open_fetches_only_the_root_and_that_collection(
        self, sample_datasets, served_catalog, mocker
    ):
        mock_open = mocker.patch("dynamical_catalog._open._open_dataset")

        dynamical_catalog.open("noaa-gfs-forecast")
        dynamical_catalog.open("noaa-gfs-forecast")

        # Fetched once each, then served from the cache.
        assert [c.args[0] for c in served_catalog.call_args_list] == [
            "https://stac.dynamical.org/catalog.json",
            "https://stac.dynamical.org/noaa-gfs-forecast/collection.json",
        ]
        mock_open.assert_called_with(sample_datasets["noaa-gfs-forecast"])

    def test_open_ignores_a_collection_this_client_cannot_read(
        self, sample_datasets, served_catalog, mocker
    ):
        _add_future_dataset(served_catalog)
        mock_open = mocker.patch("dynamical_catalog._open._open_dataset")

        dynamical_catalog.open("noaa-gfs-forecast")

        mock_open.assert_called_once_with(sample_datasets["noaa-gfs-forecast"])
        assert "future-dataset" in dynamical_catalog.list()

    def test_open_unsupported_collection_raises_with_upgrade_hint(self, served_catalog):
        _add_future_dataset(served_catalog)
        with pytest.raises(InvalidCatalogError, match="upgrad"):
            dynamical_catalog.open("future-dataset")

    def test_open_underscore_id_of_unsupported_collection_raises_its_error(
        self, served_catalog
    ):
        _add_future_dataset(served_catalog)
        with (
            pytest.warns(DeprecationWarning, match="Underscores in dataset ids"),
            pytest.raises(InvalidCatalogError, match="future-dataset"),
        ):
            dynamical_catalog.open("future_dataset")

    def test_open_ignores_an_unreachable_unrelated_collection(
        self, sample_datasets, served_catalog, mocker
    ):
        del served_catalog.responses[
            "https://stac.dynamical.org/noaa-gfs-analysis/collection.json"
        ]
        mock_open = mocker.patch("dynamical_catalog._open._open_dataset")

        dynamical_catalog.open("noaa-gfs-forecast")

        mock_open.assert_called_once_with(sample_datasets["noaa-gfs-forecast"])

    def test_a_failed_open_is_not_cached(self, sample_datasets, served_catalog, mocker):
        url = "https://stac.dynamical.org/noaa-gfs-forecast/collection.json"
        good = served_catalog.responses[url]
        served_catalog.responses[url] = {**good, "assets": {}}
        with pytest.raises(InvalidCatalogError):
            dynamical_catalog.open("noaa-gfs-forecast")

        served_catalog.responses[url] = good
        mock_open = mocker.patch("dynamical_catalog._open._open_dataset")
        dynamical_catalog.open("noaa-gfs-forecast")
        mock_open.assert_called_once_with(sample_datasets["noaa-gfs-forecast"])


class TestGetStore:
    def test_get_store_by_dataset_id(self, populated_catalog, mocker):
        mock_get_store = mocker.patch("dynamical_catalog._open._get_store")
        dynamical_catalog.get_store("noaa-gfs-forecast")
        mock_get_store.assert_called_once_with(populated_catalog["noaa-gfs-forecast"])

    def test_get_store_underscore_id_resolves_with_deprecation_warning(
        self, populated_catalog, mocker
    ):
        mock_get_store = mocker.patch("dynamical_catalog._open._get_store")
        with pytest.warns(DeprecationWarning, match="Underscores in dataset ids"):
            dynamical_catalog.get_store("noaa_gfs_forecast")
        mock_get_store.assert_called_once_with(populated_catalog["noaa-gfs-forecast"])

    def test_get_store_fetches_only_the_root_and_that_collection(
        self, sample_datasets, served_catalog, mocker
    ):
        mock_get_store = mocker.patch("dynamical_catalog._open._get_store")

        dynamical_catalog.get_store("noaa-gfs-forecast")

        assert served_catalog.call_count == 2
        mock_get_store.assert_called_once_with(sample_datasets["noaa-gfs-forecast"])


class TestGetRepository:
    def test_get_repository_by_dataset_id(self, populated_catalog, mocker):
        mock_get_repo = mocker.patch("dynamical_catalog._open._get_repository")
        dynamical_catalog.get_repository("noaa-gfs-forecast")
        mock_get_repo.assert_called_once_with(populated_catalog["noaa-gfs-forecast"])

    def test_get_repository_underscore_id_resolves_with_deprecation_warning(
        self, populated_catalog, mocker
    ):
        mock_get_repo = mocker.patch("dynamical_catalog._open._get_repository")
        with pytest.warns(DeprecationWarning, match="Underscores in dataset ids"):
            dynamical_catalog.get_repository("noaa_gfs_forecast")
        mock_get_repo.assert_called_once_with(populated_catalog["noaa-gfs-forecast"])

    def test_get_repository_fetches_only_the_root_and_that_collection(
        self, sample_datasets, served_catalog, mocker
    ):
        mock_get_repo = mocker.patch("dynamical_catalog._open._get_repository")

        dynamical_catalog.get_repository("noaa-gfs-forecast")

        assert served_catalog.call_count == 2
        mock_get_repo.assert_called_once_with(sample_datasets["noaa-gfs-forecast"])


class TestList:
    def test_list_returns_sorted_ids(self, populated_catalog):
        ids = dynamical_catalog.list()
        assert isinstance(ids, list)
        assert ids == sorted(ids)
        assert "noaa-gfs-forecast" in ids

    def test_list_fetches_only_the_root(self, sample_datasets, served_catalog):
        ids = dynamical_catalog.list()

        served_catalog.assert_called_once_with(
            "https://stac.dynamical.org/catalog.json"
        )
        assert ids == sorted(sample_datasets.keys())


class TestIdentify:
    def test_identify_sets_identifier(self):
        dynamical_catalog.identify("marshall@dynamical.org")
        assert stac._identifier == "marshall@dynamical.org"

    def test_identify_overwrites_previous_value(self):
        dynamical_catalog.identify("first@example.com")
        dynamical_catalog.identify("second@example.com")
        assert stac._identifier == "second@example.com"

    def test_identify_empty_string_disables_identification(self):
        # identify() is typed as ``str | None``; empty string and None both
        # disable identification. Empty string is normalized to None on
        # assignment so reads of _identifier are predictable.
        dynamical_catalog.identify("first@example.com")
        dynamical_catalog.identify("")
        assert stac._identifier is None
        expected = f"dynamical-catalog/{dynamical_catalog.__version__}"
        assert stac._user_agent() == expected

    def test_identify_none_disables_identification(self):
        dynamical_catalog.identify("first@example.com")
        dynamical_catalog.identify(None)
        assert stac._identifier is None
        expected = f"dynamical-catalog/{dynamical_catalog.__version__}"
        assert stac._user_agent() == expected

    def test_identify_non_string_warns_deprecated_behavior(self):
        with pytest.warns(
            DeprecationWarning,
            match="deprecated and will be removed in 1.0",
        ):
            dynamical_catalog.identify(42)  # type: ignore[arg-type]
        assert "(42)" in stac._user_agent()


class TestClearCache:
    def test_clear_cache_forces_root_and_collection_refetch(
        self, served_catalog, mocker
    ):
        mocker.patch("dynamical_catalog._open._open_dataset")
        dynamical_catalog.open("noaa-gfs-forecast")
        assert served_catalog.call_count == 2

        dynamical_catalog.clear_cache()
        dynamical_catalog.open("noaa-gfs-forecast")

        assert served_catalog.call_count == 4

    def test_clear_cache_returns_none(self):
        assert dynamical_catalog.clear_cache() is None


class TestPublicExceptions:
    def test_catalog_fetch_error_exposes_urls_and_attempts(self):
        err = dynamical_catalog.CatalogFetchError(
            "boom", urls=("https://example.com",), attempts=3
        )

        assert err.urls == ("https://example.com",)
        assert err.attempts == 3

    def test_dataset_open_error_exposes_dataset_id(self):
        err = dynamical_catalog.DatasetOpenError("boom", dataset_id="abc")

        assert err.dataset_id == "abc"


class TestPublicSurface:
    def test_all_lists_match_module_attributes(self):
        # __all__ should not advertise names that aren't actually importable.
        for name in dynamical_catalog.__all__:
            assert hasattr(dynamical_catalog, name), (
                f"__all__ lists {name!r} but module has no such attribute"
            )

    def test_expected_public_names_are_exported(self):
        # Lock in the public API so accidental removals are caught in CI.
        expected = {
            "CatalogFetchError",
            "DatasetOpenError",
            "DynamicalCatalogError",
            "InvalidCatalogError",
            "UnknownDatasetError",
            "__version__",
            "clear_cache",
            "get_repository",
            "get_store",
            "identify",
            "list",
            "open",
        }
        assert set(dynamical_catalog.__all__) == expected

    def test_version_is_non_empty(self):
        # An editable install missing package metadata would produce an empty
        # version string, which silently breaks the User-Agent header.
        assert isinstance(dynamical_catalog.__version__, str)
        assert dynamical_catalog.__version__
