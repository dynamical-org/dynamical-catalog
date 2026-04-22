"""dynamical_catalog - Load dynamical.org weather datasets in one line."""

from __future__ import annotations

from importlib.metadata import version
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import xarray as xr
    from zarr.abc.store import Store

from dynamical_catalog._stac import clear_cache, load_catalog, set_identifier

__version__ = version("dynamical-catalog")


def identify(identifier: str) -> None:
    """Set your identity for STAC catalog requests.

    The identifier (typically an email or company name) is included in
    the User-Agent header: ``dynamical-catalog/0.1.0 (identifier)``.

    Args:
        identifier: Email or company name, e.g. "marshall@dynamical.org".
    """
    set_identifier(identifier)


def get_store(dataset_id: str) -> Store:
    """Get a zarr Store for a dynamical.org dataset's icechunk repository.

    Args:
        dataset_id: Dataset identifier (e.g. "noaa-gfs-forecast").
            Underscores are also accepted (e.g. "noaa_gfs_forecast").

    Returns:
        zarr.abc.Store
    """
    from dynamical_catalog._open import _get_store

    return _get_store(_resolve(dataset_id))


def open(dataset_id: str, **kwargs: Any) -> xr.Dataset:
    """Open a dynamical.org dataset by ID.

    Args:
        dataset_id: Dataset identifier (e.g. "noaa-gfs-forecast").
            Underscores are also accepted (e.g. "noaa_gfs_forecast").
        **kwargs: Passed through to xr.open_zarr().

    Returns:
        xarray.Dataset
    """
    from dynamical_catalog._open import _open_dataset

    return _open_dataset(_resolve(dataset_id), **kwargs)


def list() -> list[str]:  # type: ignore[valid-type]
    """List available dataset IDs."""
    return sorted(load_catalog().keys())


def _resolve(dataset_id: str) -> dict[str, Any]:
    datasets = load_catalog()
    normalized_id = dataset_id.replace("_", "-")
    if normalized_id not in datasets:
        available = ", ".join(sorted(datasets.keys()))
        raise ValueError(f"Unknown dataset {dataset_id!r}. Available: {available}")
    return datasets[normalized_id]


__all__ = [
    "__version__",
    "clear_cache",
    "get_store",
    "identify",
    "list",
    "open",
]
