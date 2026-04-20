import urllib.error
from unittest.mock import patch

import pytest

import dynamical_catalog._stac as stac

_CATALOG_URL = "https://stac.dynamical.org/catalog.json"
_COLLECTION_URL = "https://stac.dynamical.org/noaa-gfs-forecast/collection.json"
_ZARR_URL = "https://data.dynamical.org/noaa/gfs/forecast/latest.zarr"

MOCK_CATALOG = {
    "type": "Catalog",
    "id": "dynamical-org",
    "stac_version": "1.0.0",
    "links": [
        {"rel": "self", "href": "https://stac.dynamical.org/catalog.json"},
        {"rel": "root", "href": "https://stac.dynamical.org/catalog.json"},
        {
            "rel": "child",
            "href": "./noaa-gfs-forecast/collection.json",
            "title": "NOAA GFS forecast",
        },
    ],
}

MOCK_COLLECTION = {
    "type": "Collection",
    "id": "noaa-gfs-forecast",
    "stac_version": "1.0.0",
    "title": "NOAA GFS forecast",
    "description": "Weather forecasts from GFS.",
    "license": "CC-BY-4.0",
    "assets": {
        "zarr": {
            "href": "https://data.dynamical.org/noaa/gfs/forecast/latest.zarr",
            "type": "application/x-zarr",
        },
        "icechunk": {
            "href": "s3://dynamical-noaa-gfs/noaa-gfs-forecast/v0.2.7.icechunk/",
            "type": "application/x-icechunk",
            "icechunk:storage": {
                "bucket": "dynamical-noaa-gfs",
                "prefix": "noaa-gfs-forecast/v0.2.7.icechunk/",
                "region": "us-west-2",
            },
        },
    },
    "links": [],
    "extent": {
        "spatial": {"bbox": [[-180, -90, 180, 90]]},
        "temporal": {"interval": [["2000-01-01T00:00:00Z", None]]},
    },
}


class TestFetchJson:
    def test_network_error_raises_runtime_error(self):
        with patch.object(
            stac.urllib.request,
            "urlopen",
            side_effect=urllib.error.URLError("connection refused"),
        ):
            with pytest.raises(RuntimeError, match="Failed to fetch"):
                stac._fetch_json("https://stac.dynamical.org/catalog.json")

    def test_http_error_raises_runtime_error(self):
        with patch.object(
            stac.urllib.request,
            "urlopen",
            side_effect=urllib.error.HTTPError(
                "https://example.com", 403, "Forbidden", {}, None
            ),
        ):
            with pytest.raises(RuntimeError, match="Failed to fetch"):
                stac._fetch_json("https://example.com")

    def test_sends_user_agent_header(self):
        """Verify that _fetch_json sends a dynamical-catalog User-Agent."""
        import dynamical_catalog

        old_id = stac._identifier
        stac._identifier = None
        try:
            with patch.object(stac.urllib.request, "urlopen") as mock_urlopen:
                mock_urlopen.return_value.__enter__ = lambda s: s
                mock_urlopen.return_value.__exit__ = lambda *a: None
                mock_urlopen.return_value.read.return_value = b"{}"

                stac._fetch_json("https://example.com")

                req = mock_urlopen.call_args[0][0]
                expected = f"dynamical-catalog/{dynamical_catalog.__version__}"
                assert req.get_header("User-agent") == expected
        finally:
            stac._identifier = old_id

    def test_user_agent_includes_identifier(self):
        import dynamical_catalog

        old_id = stac._identifier
        stac.set_identifier("test@example.com")
        try:
            with patch.object(stac.urllib.request, "urlopen") as mock_urlopen:
                mock_urlopen.return_value.__enter__ = lambda s: s
                mock_urlopen.return_value.__exit__ = lambda *a: None
                mock_urlopen.return_value.read.return_value = b"{}"

                stac._fetch_json("https://example.com")

                req = mock_urlopen.call_args[0][0]
                v = dynamical_catalog.__version__
                expected = f"dynamical-catalog/{v} (test@example.com)"
                assert req.get_header("User-agent") == expected
        finally:
            stac._identifier = old_id


class TestParseCollection:
    def test_parses_zarr_url(self):
        result = stac._parse_collection(MOCK_COLLECTION)
        assert result["zarr_url"] == _ZARR_URL

    def test_parses_icechunk_config(self):
        result = stac._parse_collection(MOCK_COLLECTION)
        assert result["icechunk"]["bucket"] == "dynamical-noaa-gfs"
        assert result["icechunk"]["prefix"] == "noaa-gfs-forecast/v0.2.7.icechunk/"
        assert result["icechunk"]["region"] == "us-west-2"

    def test_no_icechunk_asset(self):
        zarr_only = {"zarr": MOCK_COLLECTION["assets"]["zarr"]}
        collection = {**MOCK_COLLECTION, "assets": zarr_only}
        result = stac._parse_collection(collection)
        assert "icechunk" not in result

    def test_missing_zarr_asset_raises(self):
        collection = {**MOCK_COLLECTION, "assets": {}}
        with pytest.raises(ValueError, match="missing a 'zarr' asset"):
            stac._parse_collection(collection)

    def test_parses_metadata(self):
        result = stac._parse_collection(MOCK_COLLECTION)
        assert result["id"] == "noaa-gfs-forecast"
        assert result["name"] == "NOAA GFS forecast"
        assert result["description"] == "Weather forecasts from GFS."


class TestLoadCatalog:
    @pytest.fixture(autouse=True)
    def reset_cache(self):
        stac.clear_cache()
        yield
        stac.clear_cache()

    def test_loads_and_caches(self):
        responses = {
            _CATALOG_URL: MOCK_CATALOG,
            _COLLECTION_URL: MOCK_COLLECTION,
        }
        fake = patch.object(stac, "_fetch_json", side_effect=lambda url: responses[url])
        with fake:
            result = stac.load_catalog()
            assert "noaa-gfs-forecast" in result
            assert result["noaa-gfs-forecast"]["zarr_url"] == _ZARR_URL

            # Second call should use cache (no additional fetch)
            result2 = stac.load_catalog()
            assert result2 is result


class TestClearCache:
    def test_clear_cache_forces_refetch(self):
        responses = {
            _CATALOG_URL: MOCK_CATALOG,
            _COLLECTION_URL: MOCK_COLLECTION,
        }
        fake = patch.object(stac, "_fetch_json", side_effect=lambda url: responses[url])
        with fake as mock_fetch:
            stac.load_catalog()
            call_count_after_first = mock_fetch.call_count

            stac.clear_cache()
            stac.load_catalog()

            # Should have fetched again after clearing
            assert mock_fetch.call_count > call_count_after_first

        stac.clear_cache()

    def test_clear_cache_is_exported(self):
        import dynamical_catalog

        assert hasattr(dynamical_catalog, "clear_cache")
        assert dynamical_catalog.clear_cache is stac.clear_cache


class TestIdentifier:
    def test_set_identifier_updates_module_state(self):
        old_id = stac._identifier
        try:
            stac.set_identifier("acme@example.com")
            assert stac._identifier == "acme@example.com"
        finally:
            stac._identifier = old_id

    def test_identify_is_exported(self):
        import dynamical_catalog

        assert hasattr(dynamical_catalog, "identify")
