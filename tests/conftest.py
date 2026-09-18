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

# The raw STAC Collections that parse into SAMPLE_DATASETS. The catalog cache
# holds collections; a dataset's config is parsed only when it is resolved.
SAMPLE_COLLECTIONS = {
    dataset_id: {
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
}


@pytest.fixture(autouse=True)
def restore_stac_module_state():
    # Module-level globals in dynamical_catalog._stac (_datasets, _identifier)
    # leak between tests. Snapshot at start, restore at end so individual tests
    # can mutate them freely without try/finally.
    saved_datasets = stac._datasets
    saved_identifier = stac._identifier
    saved_duplicate_urls = stac._duplicate_urls
    yield
    stac._duplicate_urls = saved_duplicate_urls
    stac._datasets = saved_datasets
    stac._identifier = saved_identifier


@pytest.fixture
def sample_datasets():
    return SAMPLE_DATASETS


@pytest.fixture
def sample_collections():
    return SAMPLE_COLLECTIONS


@pytest.fixture
def populated_catalog(sample_datasets, sample_collections):
    # Pre-populate the in-process catalog cache so calls to open()/get_store()/
    # list() resolve without hitting the network. The autouse fixture above
    # restores the prior value after the test.
    stac._datasets = sample_collections
    return sample_datasets
