"""Slow integration tests against the test STAC catalog.

stac-test.dynamical.org is a superset of staging that also carries fixture
datasets which exist only to exercise this library's read paths against real
generator output. ``test-gcs-virtual`` and ``test-azure-virtual`` are icechunk
repositories on public GCS and Azure Blob Storage whose single chunk is a
virtual reference into the same bucket or container, so each covers both that
backend's repository path and its virtual chunk container path.

Marked ``testcatalog`` (as well as ``slow``); runs with the staging tests in the
non-blocking Staging integration workflow.

Run with: pytest -m testcatalog
"""

import numpy as np
import pytest
import xarray as xr

import dynamical_catalog
from dynamical_catalog._stac import CATALOG_URL_ENV_VAR, TEST_STAC_CATALOG_URL

pytestmark = [pytest.mark.slow, pytest.mark.testcatalog]

_FIXTURES = ("test-gcs-virtual", "test-azure-virtual")


@pytest.fixture(autouse=True)
def test_catalog(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(CATALOG_URL_ENV_VAR, TEST_STAC_CATALOG_URL)
    dynamical_catalog.clear_cache()
    yield
    dynamical_catalog.clear_cache()


class TestTestCatalog:
    def test_all_datasets_open(self):
        for dataset_id in dynamical_catalog.list():
            ds = dynamical_catalog.open(dataset_id)
            assert isinstance(ds, xr.Dataset), f"{dataset_id} did not return a Dataset"
            assert len(ds.data_vars) > 0, f"{dataset_id} has no data variables"


class TestFixtures:
    @pytest.mark.parametrize("dataset_id", _FIXTURES)
    def test_present(self, dataset_id: str):
        assert dataset_id in dynamical_catalog.list()

    @pytest.mark.parametrize("dataset_id", _FIXTURES)
    def test_reads_virtual_chunk(self, dataset_id: str):
        ds = dynamical_catalog.open(dataset_id)
        values = ds["temperature_2m"].values
        assert values.shape == (2, 3, 4)
        assert values.dtype == np.float32
        assert not np.isnan(values).any()
