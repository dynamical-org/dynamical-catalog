import warnings

import pytest

import dynamical_catalog
import dynamical_catalog._stac as stac
from dynamical_catalog.exceptions import (
    DynamicalCatalogError,
    UnknownDatasetError,
)


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
        # caught ValueError before the typed-exception migration keep working.
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

    def test_open_triggers_catalog_fetch_on_cold_cache(self, sample_datasets, mocker):
        # When the in-process cache is empty, calling open() should drive a
        # catalog fetch through load_catalog() rather than silently failing.
        stac._datasets = None
        mock_load = mocker.patch(
            "dynamical_catalog.load_catalog", return_value=sample_datasets
        )
        mock_open = mocker.patch("dynamical_catalog._open._open_dataset")

        dynamical_catalog.open("noaa-gfs-forecast")

        mock_load.assert_called_once()
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

    def test_get_store_triggers_catalog_fetch_on_cold_cache(
        self, sample_datasets, mocker
    ):
        stac._datasets = None
        mock_load = mocker.patch(
            "dynamical_catalog.load_catalog", return_value=sample_datasets
        )
        mock_get_store = mocker.patch("dynamical_catalog._open._get_store")

        dynamical_catalog.get_store("noaa-gfs-forecast")

        mock_load.assert_called_once()
        mock_get_store.assert_called_once_with(sample_datasets["noaa-gfs-forecast"])


class TestList:
    def test_list_returns_sorted_ids(self, populated_catalog):
        ids = dynamical_catalog.list()
        assert isinstance(ids, list)
        assert ids == sorted(ids)
        assert "noaa-gfs-forecast" in ids

    def test_list_triggers_catalog_fetch_on_cold_cache(self, sample_datasets, mocker):
        stac._datasets = None
        mock_load = mocker.patch(
            "dynamical_catalog.load_catalog", return_value=sample_datasets
        )

        ids = dynamical_catalog.list()

        mock_load.assert_called_once()
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

    def test_identify_non_string_is_coerced(self):
        # Pin current behavior: identify() is typed as ``str | None`` and
        # callers passing other types are misusing the API. f-string
        # interpolation in _user_agent silently coerces non-strings; this
        # test documents that as a regression guard, not as an endorsed path.
        dynamical_catalog.identify(42)  # type: ignore[arg-type]
        assert "(42)" in stac._user_agent()


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
