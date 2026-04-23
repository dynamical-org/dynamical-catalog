import pytest

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


@pytest.fixture
def sample_datasets():
    return SAMPLE_DATASETS
