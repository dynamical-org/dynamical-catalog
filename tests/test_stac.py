from unittest.mock import patch

import pytest

import dynamical._stac as stac


MOCK_CATALOG = {
    "type": "Catalog",
    "id": "dynamical-org",
    "stac_version": "1.0.0",
    "links": [
        {"rel": "self", "href": "https://dynamical.org/stac/catalog.json"},
        {"rel": "root", "href": "https://dynamical.org/stac/catalog.json"},
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


class TestParseCollection:
    def test_parses_zarr_url(self):
        result = stac._parse_collection(MOCK_COLLECTION)
        assert result["zarr_url"] == "https://data.dynamical.org/noaa/gfs/forecast/latest.zarr"

    def test_parses_icechunk_config(self):
        result = stac._parse_collection(MOCK_COLLECTION)
        assert result["icechunk"]["bucket"] == "dynamical-noaa-gfs"
        assert result["icechunk"]["prefix"] == "noaa-gfs-forecast/v0.2.7.icechunk/"
        assert result["icechunk"]["region"] == "us-west-2"

    def test_no_icechunk_asset(self):
        collection = {**MOCK_COLLECTION, "assets": {"zarr": MOCK_COLLECTION["assets"]["zarr"]}}
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
        stac._datasets = None
        yield
        stac._datasets = None

    def test_loads_and_caches(self):
        responses = {
            "https://dynamical.org/stac/catalog.json": MOCK_CATALOG,
            "https://dynamical.org/stac/noaa-gfs-forecast/collection.json": MOCK_COLLECTION,
        }

        with patch.object(stac, "_fetch_json", side_effect=lambda url: responses[url]):
            result = stac.load_catalog()
            assert "noaa-gfs-forecast" in result
            assert result["noaa-gfs-forecast"]["zarr_url"] == "https://data.dynamical.org/noaa/gfs/forecast/latest.zarr"

            # Second call should use cache (no additional fetch)
            result2 = stac.load_catalog()
            assert result2 is result
