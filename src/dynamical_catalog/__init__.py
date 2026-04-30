"""dynamical_catalog - Load dynamical.org weather datasets in one line."""

from __future__ import annotations

from importlib.metadata import version
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import xarray as xr
    from zarr.abc.store import Store

from dynamical_catalog._stac import clear_cache, load_catalog, set_identifier
from dynamical_catalog.exceptions import (
    CatalogFetchError,
    DatasetOpenError,
    DynamicalCatalogError,
    InvalidCatalogError,
    UnknownDatasetError,
)

__version__ = version("dynamical-catalog")


def identify(identifier: str | None) -> None:
    """Set a user identifier to help dynamical.org improve the catalog.

    Passing an empty string or ``None`` disables identification.

    Args:
        identifier: Email or company name (e.g. ``"you@example.com"``), or
            ``None`` / ``""`` to disable identification.
    """
    set_identifier(identifier)


def get_store(dataset_id: str) -> Store:
    """Get a zarr Store for a dynamical.org dataset's icechunk repository.

    On the first call (per process) this fetches the STAC catalog from
    dynamical.org; subsequent calls reuse the in-process cache.

    Args:
        dataset_id: Dataset identifier (e.g. ``"noaa-gfs-forecast"``).

    Returns:
        A read-only :class:`zarr.abc.store.Store` backed by the dataset's
        icechunk repository.

    Raises:
        UnknownDatasetError: ``dataset_id`` is not in the catalog.
        CatalogFetchError: Fetching the STAC catalog failed.
        InvalidCatalogError: The catalog response was reachable but malformed.
        DatasetOpenError: Opening the icechunk repository failed.
    """
    from dynamical_catalog._open import _get_store

    return _get_store(_resolve(dataset_id))


def open(dataset_id: str, **kwargs: Any) -> xr.Dataset:
    """Open a dynamical.org dataset by ID as an :class:`xarray.Dataset`.

    On the first call (per process) this fetches the STAC catalog from
    dynamical.org; subsequent calls reuse the in-process cache.

    Args:
        dataset_id: Dataset identifier (e.g. ``"noaa-gfs-forecast"``).
        **kwargs: Passed through to :func:`xarray.open_zarr`.

    Returns:
        The dataset as an :class:`xarray.Dataset`.

    Raises:
        UnknownDatasetError: ``dataset_id`` is not in the catalog.
        CatalogFetchError: Fetching the STAC catalog failed.
        InvalidCatalogError: The catalog response was reachable but malformed.
        DatasetOpenError: Opening the icechunk repository or zarr store
            failed. This covers eager errors at open time only; errors
            raised later when reading data lazily from the returned
            dataset are not wrapped.
    """
    from dynamical_catalog._open import _open_dataset

    return _open_dataset(_resolve(dataset_id), **kwargs)


def list() -> list[str]:  # type: ignore[valid-type]
    """List available dataset IDs, sorted alphabetically.

    On the first call (per process) this fetches the STAC catalog from
    dynamical.org; subsequent calls reuse the in-process cache.

    Returns:
        Sorted list of dataset IDs.

    Raises:
        CatalogFetchError: Fetching the STAC catalog failed.
        InvalidCatalogError: The catalog response was reachable but malformed.
    """
    return sorted(load_catalog().keys())


def _resolve(dataset_id: str) -> dict[str, Any]:
    datasets = load_catalog()
    if dataset_id not in datasets:
        available = ", ".join(sorted(datasets.keys()))
        raise UnknownDatasetError(
            f"Unknown dataset {dataset_id!r}. Available: {available}"
        )
    return datasets[dataset_id]


__all__ = [
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
]
