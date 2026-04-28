import pytest

import dynamical_catalog._stac as stac

SAMPLE_DATASETS = {
    "noaa-gfs-forecast": {
        "id": "noaa-gfs-forecast",
        "name": "NOAA GFS forecast",
        "description": "Weather forecasts from GFS.",
        "icechunk": {
            "bucket": "dynamical-noaa-gfs",
            "prefix": "noaa-gfs-forecast/v0.2.7.icechunk/",
            "region": "us-west-2",
        },
    },
    "noaa-gfs-analysis": {
        "id": "noaa-gfs-analysis",
        "name": "NOAA GFS analysis",
        "description": "Weather analysis from GFS.",
        "icechunk": {
            "bucket": "dynamical-noaa-gfs",
            "prefix": "noaa-gfs-analysis/v0.1.0.icechunk/",
            "region": "us-west-2",
        },
    },
    "noaa-gefs-forecast-35-day": {
        "id": "noaa-gefs-forecast-35-day",
        "name": "NOAA GEFS forecast, 35 day",
        "description": "Ensemble forecasts from GEFS.",
        "icechunk": {
            "bucket": "dynamical-noaa-gefs",
            "prefix": "noaa-gefs-forecast-35-day/v0.2.0.icechunk/",
            "region": "us-west-2",
        },
    },
}


@pytest.fixture(autouse=True)
def restore_stac_module_state():
    # Module-level globals in dynamical_catalog._stac (_datasets, _identifier)
    # leak between tests. Snapshot at start, restore at end so individual tests
    # can mutate them freely without try/finally.
    saved_datasets = stac._datasets
    saved_identifier = stac._identifier
    yield
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
    stac._datasets = sample_datasets
    return sample_datasets
