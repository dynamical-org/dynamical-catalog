import pytest

import dynamical_catalog._stac as stac

SAMPLE_DATASETS = {
    "noaa-gfs-forecast": {
        "id": "noaa-gfs-forecast",
        "name": "NOAA GFS forecast",
        "description": "Weather forecasts from GFS.",
        "icechunk": {
            "type": "s3",
            "bucket": "dynamical-noaa-gfs",
            "prefix": "noaa-gfs-forecast/v0.2.7.icechunk/",
            "region": "us-west-2",
        },
        "virtual_chunk_containers": [],
    },
    "noaa-gfs-analysis": {
        "id": "noaa-gfs-analysis",
        "name": "NOAA GFS analysis",
        "description": "Weather analysis from GFS.",
        "icechunk": {
            "type": "s3",
            "bucket": "dynamical-noaa-gfs",
            "prefix": "noaa-gfs-analysis/v0.1.0.icechunk/",
            "region": "us-west-2",
        },
        "virtual_chunk_containers": [],
    },
    "noaa-gefs-forecast-35-day": {
        "id": "noaa-gefs-forecast-35-day",
        "name": "NOAA GEFS forecast, 35 day",
        "description": "Ensemble forecasts from GEFS.",
        "icechunk": {
            "type": "s3",
            "bucket": "dynamical-noaa-gefs",
            "prefix": "noaa-gefs-forecast-35-day/v0.2.0.icechunk/",
            "region": "us-west-2",
        },
        "virtual_chunk_containers": [],
    },
}

_ROOT_URL = "https://stac.dynamical.org/catalog.json"


def _collection_url(dataset_id: str) -> str:
    return f"https://stac.dynamical.org/{dataset_id}/collection.json"


# What the catalog serves for SAMPLE_DATASETS, by URL.
SAMPLE_RESPONSES = {
    _ROOT_URL: {
        "type": "Catalog",
        "links": [
            {"rel": "root", "href": _ROOT_URL},
            *(
                {"rel": "child", "href": _collection_url(dataset_id)}
                for dataset_id in SAMPLE_DATASETS
            ),
        ],
    },
    **{
        _collection_url(dataset_id): {
            "type": "Collection",
            "id": dataset_id,
            "title": dataset["name"],
            "description": dataset["description"],
            "assets": {
                "icechunk": {
                    "href": (
                        f"s3://{dataset['icechunk']['bucket']}/"
                        f"{dataset['icechunk']['prefix']}"
                    ),
                    "xarray:storage_options": {
                        "client_kwargs": {"region_name": dataset["icechunk"]["region"]}
                    },
                }
            },
        }
        for dataset_id, dataset in SAMPLE_DATASETS.items()
    },
}


@pytest.fixture(autouse=True)
def restore_stac_module_state():
    # Module-level globals in dynamical_catalog._stac (_collection_urls,
    # _datasets, _identifier) leak between tests. Snapshot at start, restore at
    # end so individual tests can mutate them freely without try/finally.
    saved_collection_urls = stac._collection_urls
    saved_datasets = stac._datasets
    saved_identifier = stac._identifier
    stac._datasets = {}
    yield
    stac._collection_urls = saved_collection_urls
    stac._datasets = saved_datasets
    stac._identifier = saved_identifier


@pytest.fixture
def sample_datasets():
    return SAMPLE_DATASETS


@pytest.fixture
def populated_catalog(sample_datasets):
    # Pre-populate the in-process catalog cache so calls to open()/get_store()/
    # list() resolve without hitting the network. The autouse fixture above
    # restores the prior value after the test.
    stac._collection_urls = {
        dataset_id: _collection_url(dataset_id) for dataset_id in sample_datasets
    }
    stac._datasets = dict(sample_datasets)
    return sample_datasets


@pytest.fixture
def served_catalog(mocker):
    """A cold cache in front of a catalog serving SAMPLE_DATASETS.

    Returns the ``_fetch_json`` mock, whose calls show what was fetched. Tests
    may add to or replace entries of ``responses`` on it.
    """
    stac.clear_cache()
    responses = dict(SAMPLE_RESPONSES)
    mock_fetch = mocker.patch.object(
        stac, "_fetch_json", side_effect=lambda url: responses[url]
    )
    mock_fetch.responses = responses
    return mock_fetch
