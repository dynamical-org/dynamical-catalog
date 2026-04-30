import http.client
import urllib.error
from unittest.mock import MagicMock, call

import pytest

import dynamical_catalog
import dynamical_catalog._stac as stac
from dynamical_catalog.exceptions import (
    CatalogFetchError,
    DynamicalCatalogError,
    InvalidCatalogError,
)

_CATALOG_URL = "https://stac.dynamical.org/catalog.json"
_COLLECTION_URL = "https://stac.dynamical.org/noaa-gfs-forecast/collection.json"

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
        "icechunk": {
            "href": "s3://dynamical-noaa-gfs/noaa-gfs-forecast/v0.2.7.icechunk/",
            "type": "application/x-icechunk",
            "xarray:storage_options": {
                "anon": True,
                "client_kwargs": {"region_name": "us-west-2"},
            },
        },
    },
    "links": [],
    "extent": {
        "spatial": {"bbox": [[-180, -90, 180, 90]]},
        "temporal": {"interval": [["2000-01-01T00:00:00Z", None]]},
    },
}


def _mock_urlopen_response(mocker, body: bytes):
    """Patch urllib.request.urlopen to return a context manager yielding body."""
    mock_urlopen = mocker.patch.object(stac.urllib.request, "urlopen")
    mock_urlopen.return_value.__enter__ = lambda s: s
    mock_urlopen.return_value.__exit__ = lambda *a: None
    mock_urlopen.return_value.read.return_value = body
    return mock_urlopen


class TestFetchJson:
    def test_network_error_raises_catalog_fetch_error(self, mocker):
        mocker.patch.object(
            stac.urllib.request,
            "urlopen",
            side_effect=urllib.error.URLError("connection refused"),
        )
        with pytest.raises(CatalogFetchError, match="Failed to fetch") as excinfo:
            stac._fetch_json(_CATALOG_URL)
        assert excinfo.value.urls == (_CATALOG_URL,)
        assert excinfo.value.attempts == stac._MAX_ATTEMPTS
        assert isinstance(excinfo.value, DynamicalCatalogError)

    def test_http_4xx_raises_catalog_fetch_error_without_retry(self, mocker):
        mocker.patch.object(stac.time, "sleep")
        mock_urlopen = mocker.patch.object(
            stac.urllib.request,
            "urlopen",
            side_effect=urllib.error.HTTPError(
                "https://example.com", 403, "Forbidden", {}, None
            ),
        )
        with pytest.raises(CatalogFetchError, match="HTTP 403"):
            stac._fetch_json("https://example.com")
        assert mock_urlopen.call_count == 1

    def test_http_429_is_retried(self, mocker):
        mocker.patch.object(stac.time, "sleep")
        mock_urlopen = mocker.patch.object(
            stac.urllib.request,
            "urlopen",
            side_effect=urllib.error.HTTPError(
                "https://example.com", 429, "Too Many Requests", {}, None
            ),
        )
        with pytest.raises(CatalogFetchError):
            stac._fetch_json("https://example.com")
        assert mock_urlopen.call_count == stac._MAX_ATTEMPTS

    def test_http_5xx_is_retried(self, mocker):
        mocker.patch.object(stac.time, "sleep")
        mock_urlopen = mocker.patch.object(
            stac.urllib.request,
            "urlopen",
            side_effect=urllib.error.HTTPError(
                "https://example.com", 503, "Service Unavailable", {}, None
            ),
        )
        with pytest.raises(CatalogFetchError):
            stac._fetch_json("https://example.com")
        assert mock_urlopen.call_count == stac._MAX_ATTEMPTS

    def test_timeout_error_is_retried(self, mocker):
        mocker.patch.object(stac.time, "sleep")
        mock_urlopen = mocker.patch.object(
            stac.urllib.request,
            "urlopen",
            side_effect=TimeoutError("read timed out"),
        )
        with pytest.raises(CatalogFetchError):
            stac._fetch_json("https://example.com")
        assert mock_urlopen.call_count == stac._MAX_ATTEMPTS

    def test_remote_disconnected_is_retried(self, mocker):
        mocker.patch.object(stac.time, "sleep")
        mock_urlopen = mocker.patch.object(
            stac.urllib.request,
            "urlopen",
            side_effect=http.client.RemoteDisconnected("server closed connection"),
        )
        with pytest.raises(CatalogFetchError):
            stac._fetch_json("https://example.com")
        assert mock_urlopen.call_count == stac._MAX_ATTEMPTS

    def test_incomplete_read_is_retried(self, mocker):
        mocker.patch.object(stac.time, "sleep")
        mock_urlopen = mocker.patch.object(
            stac.urllib.request,
            "urlopen",
            side_effect=http.client.IncompleteRead(b"partial", expected=42),
        )
        with pytest.raises(CatalogFetchError):
            stac._fetch_json("https://example.com")
        assert mock_urlopen.call_count == stac._MAX_ATTEMPTS

    def test_retries_until_max_attempts_on_persistent_failure(self, mocker):
        mocker.patch.object(stac.time, "sleep")
        mock_urlopen = mocker.patch.object(
            stac.urllib.request,
            "urlopen",
            side_effect=urllib.error.URLError("connection refused"),
        )
        with pytest.raises(CatalogFetchError):
            stac._fetch_json("https://example.com")
        assert mock_urlopen.call_count == stac._MAX_ATTEMPTS

    def test_succeeds_after_transient_failures(self, mocker):
        mocker.patch.object(stac.time, "sleep")
        success_response = MagicMock()
        success_response.__enter__ = lambda s: s
        success_response.__exit__ = lambda *a: None
        success_response.read.return_value = b'{"ok": true}'
        mocker.patch.object(
            stac.urllib.request,
            "urlopen",
            side_effect=[
                urllib.error.URLError("transient 1"),
                urllib.error.URLError("transient 2"),
                success_response,
            ],
        )
        result = stac._fetch_json("https://example.com")
        assert result == {"ok": True}

    def test_sleeps_between_attempts(self, mocker):
        mock_sleep = mocker.patch.object(stac.time, "sleep")
        mocker.patch.object(
            stac.urllib.request,
            "urlopen",
            side_effect=urllib.error.URLError("connection refused"),
        )
        with pytest.raises(CatalogFetchError):
            stac._fetch_json("https://example.com")
        # Sleeps between attempts but not after the final one.
        expected_sleeps = stac._MAX_ATTEMPTS - 1
        assert mock_sleep.call_count == expected_sleeps
        assert (
            mock_sleep.call_args_list
            == [call(stac._RETRY_BACKOFF_SECONDS)] * expected_sleeps
        )

    def test_malformed_json_response_raises_catalog_fetch_error(self, mocker):
        # Malformed JSON is wrapped as CatalogFetchError so callers see a
        # single exception type, but it is NOT retried — a malformed response
        # body won't change between attempts.
        _mock_urlopen_response(mocker, b"not json at all")
        with pytest.raises(CatalogFetchError, match="not valid JSON"):
            stac._fetch_json("https://example.com")

    def test_malformed_json_is_not_retried(self, mocker):
        mock_sleep = mocker.patch.object(stac.time, "sleep")
        mock_urlopen = _mock_urlopen_response(mocker, b"not json")
        with pytest.raises(CatalogFetchError):
            stac._fetch_json("https://example.com")
        assert mock_urlopen.call_count == 1
        mock_sleep.assert_not_called()

    def test_uses_configured_timeout(self, mocker):
        mock_urlopen = _mock_urlopen_response(mocker, b"{}")
        stac._fetch_json("https://example.com")
        _, kwargs = mock_urlopen.call_args
        assert kwargs.get("timeout") == stac._TIMEOUT_SECONDS

    def test_sends_user_agent_header(self, mocker):
        stac._identifier = None
        mock_urlopen = _mock_urlopen_response(mocker, b"{}")
        stac._fetch_json("https://example.com")
        req = mock_urlopen.call_args[0][0]
        expected = f"dynamical-catalog/{dynamical_catalog.__version__}"
        assert req.get_header("User-agent") == expected

    def test_user_agent_includes_identifier(self, mocker):
        stac.set_identifier("test@example.com")
        mock_urlopen = _mock_urlopen_response(mocker, b"{}")
        stac._fetch_json("https://example.com")
        req = mock_urlopen.call_args[0][0]
        v = dynamical_catalog.__version__
        expected = f"dynamical-catalog/{v} (test@example.com)"
        assert req.get_header("User-agent") == expected


class TestParseCollection:
    def test_parses_icechunk_config(self):
        result = stac._parse_collection(MOCK_COLLECTION)
        assert result["icechunk"]["bucket"] == "dynamical-noaa-gfs"
        assert result["icechunk"]["prefix"] == "noaa-gfs-forecast/v0.2.7.icechunk/"
        assert result["icechunk"]["region"] == "us-west-2"

    def test_parses_metadata(self):
        result = stac._parse_collection(MOCK_COLLECTION)
        assert result["id"] == "noaa-gfs-forecast"
        assert result["name"] == "NOAA GFS forecast"
        assert result["description"] == "Weather forecasts from GFS."

    def test_falls_back_to_id_when_title_missing(self):
        collection = {**MOCK_COLLECTION}
        del collection["title"]
        result = stac._parse_collection(collection)
        assert result["name"] == "noaa-gfs-forecast"

    def test_description_defaults_to_empty_string(self):
        collection = {**MOCK_COLLECTION}
        del collection["description"]
        result = stac._parse_collection(collection)
        assert result["description"] == ""

    def test_missing_icechunk_asset_raises(self):
        collection = {**MOCK_COLLECTION, "assets": {}}
        with pytest.raises(InvalidCatalogError, match="missing an 'icechunk' asset"):
            stac._parse_collection(collection)

    def test_missing_id_raises_invalid_catalog_error(self):
        collection = {k: v for k, v in MOCK_COLLECTION.items() if k != "id"}
        with pytest.raises(InvalidCatalogError, match="missing 'id'"):
            stac._parse_collection(collection)

    def test_missing_assets_raises_invalid_catalog_error(self):
        collection = {k: v for k, v in MOCK_COLLECTION.items() if k != "assets"}
        with pytest.raises(InvalidCatalogError, match="missing 'assets'"):
            stac._parse_collection(collection)

    def test_non_s3_href_raises(self):
        bad = {
            **MOCK_COLLECTION,
            "assets": {
                "icechunk": {
                    "href": "https://example.com/repo",
                    "xarray:storage_options": {
                        "client_kwargs": {"region_name": "us-west-2"},
                    },
                }
            },
        }
        with pytest.raises(InvalidCatalogError, match="scheme is not s3"):
            stac._parse_collection(bad)

    def test_empty_prefix_raises_missing_prefix(self):
        bad = {
            **MOCK_COLLECTION,
            "assets": {
                "icechunk": {
                    "href": "s3://bucket/",
                    "xarray:storage_options": {
                        "client_kwargs": {"region_name": "us-west-2"},
                    },
                }
            },
        }
        with pytest.raises(InvalidCatalogError, match="missing a prefix"):
            stac._parse_collection(bad)

    def test_empty_bucket_raises_missing_bucket(self):
        bad = {
            **MOCK_COLLECTION,
            "assets": {
                "icechunk": {
                    "href": "s3:///prefix/",
                    "xarray:storage_options": {
                        "client_kwargs": {"region_name": "us-west-2"},
                    },
                }
            },
        }
        with pytest.raises(InvalidCatalogError, match="missing a bucket"):
            stac._parse_collection(bad)

    def test_missing_region_raises(self):
        bad = {
            **MOCK_COLLECTION,
            "assets": {
                "icechunk": {
                    "href": "s3://bucket/prefix/",
                }
            },
        }
        with pytest.raises(InvalidCatalogError, match="region_name"):
            stac._parse_collection(bad)

    def test_missing_storage_options_raises_via_region_check(self):
        bad = {
            **MOCK_COLLECTION,
            "assets": {
                "icechunk": {"href": "s3://bucket/prefix/"},
            },
        }
        with pytest.raises(InvalidCatalogError, match="region_name"):
            stac._parse_collection(bad)


class TestParseVirtualChunkContainers:
    def _collection_with_containers(self, containers):
        return {
            **MOCK_COLLECTION,
            "assets": {
                "icechunk": {
                    **MOCK_COLLECTION["assets"]["icechunk"],
                    "icechunk:virtual_chunk_containers": containers,
                }
            },
        }

    def test_absent_yields_empty_list(self):
        result = stac._parse_collection(MOCK_COLLECTION)
        assert result["virtual_chunk_containers"] == []

    def test_parses_anonymous_s3_containers(self):
        collection = self._collection_with_containers(
            [
                {
                    "url_prefix": "s3://noaa-gfs-bdp-pds",
                    "credentials": {"type": "s3", "anonymous": True},
                },
                {
                    "url_prefix": "s3://some-other-bucket/path",
                    "credentials": {"type": "s3", "anonymous": True},
                },
            ]
        )
        result = stac._parse_collection(collection)
        assert result["virtual_chunk_containers"] == [
            "s3://noaa-gfs-bdp-pds",
            "s3://some-other-bucket/path",
        ]

    def test_non_s3_prefix_raises(self):
        collection = self._collection_with_containers(
            [
                {
                    "url_prefix": "https://example.com/bucket",
                    "credentials": {"type": "s3", "anonymous": True},
                }
            ]
        )
        with pytest.raises(
            InvalidCatalogError, match="url_prefix must be an s3:// string"
        ):
            stac._parse_collection(collection)

    def test_non_string_prefix_raises(self):
        collection = self._collection_with_containers(
            [
                {
                    "url_prefix": 42,
                    "credentials": {"type": "s3", "anonymous": True},
                }
            ]
        )
        with pytest.raises(
            InvalidCatalogError, match="url_prefix must be an s3:// string"
        ):
            stac._parse_collection(collection)

    def test_non_anonymous_credentials_raises(self):
        collection = self._collection_with_containers(
            [
                {
                    "url_prefix": "s3://somebucket",
                    "credentials": {
                        "type": "s3",
                        "access_key_id": "AKIA...",
                        "secret_access_key": "...",
                    },
                }
            ]
        )
        with pytest.raises(InvalidCatalogError, match="anonymous: true"):
            stac._parse_collection(collection)

    def test_missing_credentials_raises(self):
        collection = self._collection_with_containers(
            [{"url_prefix": "s3://somebucket"}]
        )
        with pytest.raises(InvalidCatalogError, match="anonymous: true"):
            stac._parse_collection(collection)

    def test_non_s3_credential_type_raises(self):
        collection = self._collection_with_containers(
            [
                {
                    "url_prefix": "s3://somebucket",
                    "credentials": {"type": "gcs", "anonymous": True},
                }
            ]
        )
        with pytest.raises(InvalidCatalogError, match="anonymous: true"):
            stac._parse_collection(collection)

    def test_none_value_treated_as_empty(self):
        # An explicit None value for the containers key is treated as empty
        # (the same as the key being absent).
        collection = self._collection_with_containers(None)
        result = stac._parse_collection(collection)
        assert result["virtual_chunk_containers"] == []

    def test_dict_value_raises_invalid_catalog_error(self):
        # A non-list, non-None value (e.g., a dict accidentally written in
        # the STAC) raises InvalidCatalogError with a clear message.
        collection = self._collection_with_containers({"url_prefix": "s3://x"})
        with pytest.raises(InvalidCatalogError, match="must be a list"):
            stac._parse_collection(collection)


class TestLoadCatalog:
    def test_loads_and_caches(self, mocker):
        responses = {
            _CATALOG_URL: MOCK_CATALOG,
            _COLLECTION_URL: MOCK_COLLECTION,
        }
        mocker.patch.object(stac, "_fetch_json", side_effect=lambda url: responses[url])

        result = stac.load_catalog()
        assert "noaa-gfs-forecast" in result
        assert result["noaa-gfs-forecast"]["icechunk"]["region"] == "us-west-2"

        # Second call should use cache (no additional fetch)
        result2 = stac.load_catalog()
        assert result2 is result

    def test_missing_links_raises_invalid_catalog_error(self, mocker):
        catalog_without_links = {k: v for k, v in MOCK_CATALOG.items() if k != "links"}
        mocker.patch.object(stac, "_fetch_json", return_value=catalog_without_links)
        with pytest.raises(InvalidCatalogError, match="missing 'links'"):
            stac.load_catalog()

    def test_empty_catalog_returns_empty_dict(self, mocker):
        # An empty catalog (no child links) is a valid state and returns {}.
        empty_catalog = {**MOCK_CATALOG, "links": []}
        mocker.patch.object(stac, "_fetch_json", return_value=empty_catalog)
        result = stac.load_catalog()
        assert result == {}

    def test_duplicate_ids_raise_invalid_catalog_error(self, mocker):
        catalog = {
            **MOCK_CATALOG,
            "links": [
                {"rel": "child", "href": "./first/collection.json"},
                {"rel": "child", "href": "./second/collection.json"},
            ],
        }
        first = {
            **MOCK_COLLECTION,
            "description": "first",
            "assets": {
                "icechunk": {
                    **MOCK_COLLECTION["assets"]["icechunk"],
                    "href": "s3://bucket/first/",
                }
            },
        }
        second = {
            **MOCK_COLLECTION,
            "description": "second",
            "assets": {
                "icechunk": {
                    **MOCK_COLLECTION["assets"]["icechunk"],
                    "href": "s3://bucket/second/",
                }
            },
        }
        responses = {
            _CATALOG_URL: catalog,
            "https://stac.dynamical.org/first/collection.json": first,
            "https://stac.dynamical.org/second/collection.json": second,
        }
        mocker.patch.object(stac, "_fetch_json", side_effect=lambda url: responses[url])

        with pytest.raises(InvalidCatalogError, match="duplicate dataset id"):
            stac.load_catalog()

    def test_failing_child_fetch_raises_catalog_fetch_error(self, mocker):
        # A failing child fetch is gathered into a single CatalogFetchError
        # listing every failed URL.
        def fetch(url):
            if url == _CATALOG_URL:
                return MOCK_CATALOG
            raise CatalogFetchError(
                f"Failed to fetch {url}", urls=(url,), attempts=stac._MAX_ATTEMPTS
            )

        mocker.patch.object(stac, "_fetch_json", side_effect=fetch)
        with pytest.raises(CatalogFetchError) as excinfo:
            stac.load_catalog()
        assert excinfo.value.urls == (_COLLECTION_URL,)

    def test_failing_child_fetch_lists_all_failed_urls(self, mocker):
        catalog = {
            **MOCK_CATALOG,
            "links": [
                {"rel": "child", "href": "./first/collection.json"},
                {"rel": "child", "href": "./second/collection.json"},
            ],
        }

        def fetch(url):
            if url == _CATALOG_URL:
                return catalog
            raise CatalogFetchError(
                f"Failed to fetch {url}", urls=(url,), attempts=stac._MAX_ATTEMPTS
            )

        mocker.patch.object(stac, "_fetch_json", side_effect=fetch)
        with pytest.raises(CatalogFetchError) as excinfo:
            stac.load_catalog()
        # urls is a tuple of every failed collection URL (order not guaranteed
        # because as_completed schedules them).
        assert set(excinfo.value.urls) == {
            "https://stac.dynamical.org/first/collection.json",
            "https://stac.dynamical.org/second/collection.json",
        }


class TestClearCache:
    def test_clear_cache_forces_refetch(self, mocker):
        responses = {
            _CATALOG_URL: MOCK_CATALOG,
            _COLLECTION_URL: MOCK_COLLECTION,
        }
        mock_fetch = mocker.patch.object(
            stac, "_fetch_json", side_effect=lambda url: responses[url]
        )
        stac.load_catalog()
        call_count_after_first = mock_fetch.call_count

        stac.clear_cache()
        stac.load_catalog()

        # Should have fetched again after clearing
        assert mock_fetch.call_count > call_count_after_first

    def test_clear_cache_is_exported(self):
        assert hasattr(dynamical_catalog, "clear_cache")
        assert dynamical_catalog.clear_cache is stac.clear_cache


class TestIdentifier:
    def test_set_identifier_updates_module_state(self):
        stac.set_identifier("acme@example.com")
        assert stac._identifier == "acme@example.com"

    def test_identify_is_exported(self):
        assert hasattr(dynamical_catalog, "identify")

    def test_user_agent_omits_parens_when_identifier_is_none(self):
        stac._identifier = None
        ua = stac._user_agent()
        assert "(" not in ua
        assert ua == f"dynamical-catalog/{dynamical_catalog.__version__}"

    def test_user_agent_omits_parens_when_identifier_is_empty(self):
        # set_identifier normalizes "" to None; _user_agent treats falsy
        # identifiers as disabled.
        stac.set_identifier("")
        ua = stac._user_agent()
        assert stac._identifier is None
        assert "(" not in ua
        assert ua == f"dynamical-catalog/{dynamical_catalog.__version__}"

    def test_set_identifier_normalizes_empty_string_to_none(self):
        stac.set_identifier("acme@example.com")
        stac.set_identifier("")
        assert stac._identifier is None

    def test_set_identifier_accepts_none(self):
        stac.set_identifier("acme@example.com")
        stac.set_identifier(None)
        assert stac._identifier is None
