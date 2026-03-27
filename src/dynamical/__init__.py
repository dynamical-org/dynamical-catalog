"""dynamical - Load dynamical.org weather datasets in one line."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import xarray as xr

from dynamical._catalog import Catalog
from dynamical._stac import clear_cache, load_catalog

__version__ = "0.1.0"

catalog = Catalog(load_catalog)


def open(dataset_id: str, engine: str = "zarr", **kwargs: Any) -> xr.Dataset:
    """Open a dynamical.org dataset by ID.

    Args:
        dataset_id: Dataset identifier (e.g. "noaa-gfs-forecast").
            Underscores are also accepted (e.g. "noaa_gfs_forecast").
        engine: "zarr" (default) or "icechunk".
        **kwargs: Passed through to xr.open_zarr().

    Returns:
        xarray.Dataset
    """
    from dynamical._open import _open_dataset

    datasets = load_catalog()
    normalized_id = dataset_id.replace("_", "-")
    if normalized_id not in datasets:
        available = ", ".join(sorted(datasets.keys()))
        raise ValueError(
            f"Unknown dataset {dataset_id!r}. Available: {available}"
        )
    return _open_dataset(datasets[normalized_id], engine=engine, **kwargs)


def list() -> list[str]:
    """List available dataset IDs."""
    return sorted(load_catalog().keys())


__all__ = ["open", "list", "catalog", "clear_cache", "__version__"]
