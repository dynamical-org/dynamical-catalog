"""dynamical_catalog - Load dynamical.org weather datasets in one line."""

from __future__ import annotations

import warnings
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

    Args:
        identifier: Email or company name (e.g. ``"you@example.com"``), or
            ``None`` / ``""`` to disable identification.
    """
    if identifier is not None and not isinstance(identifier, str):
        warnings.warn(
            "Passing non-str identifiers to identify() is deprecated and will be removed in 1.0; "
            "pass str or None.",
            DeprecationWarning,
            stacklevel=2,
        )
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
    if dataset_id in datasets:
        return datasets[dataset_id]
    # Underscore form is deprecated but still accepted when it resolves to a
    # real id. Only warn on resolved hits — a typo with underscores should
    # surface as UnknownDatasetError, not a deprecation notice.
    if "_" in dataset_id:
        normalized_id = dataset_id.replace("_", "-")
        if normalized_id in datasets:
            warnings.warn(
                f"Underscores in dataset ids are deprecated and will be "
                f"removed in 1.0; use {normalized_id!r} instead of "
                f"{dataset_id!r}.",
                DeprecationWarning,
                stacklevel=3,
            )
            return datasets[normalized_id]
    available = ", ".join(sorted(datasets.keys()))
    raise UnknownDatasetError(f"Unknown dataset {dataset_id!r}. Available: {available}")


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
