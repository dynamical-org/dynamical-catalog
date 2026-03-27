from __future__ import annotations

from typing import Any

import xarray as xr


def _open_dataset(
    dataset_data: dict[str, Any],
    engine: str = "zarr",
    **kwargs: Any,
) -> xr.Dataset:
    if engine == "zarr":
        return _open_zarr(dataset_data, **kwargs)
    if engine == "icechunk":
        return _open_icechunk(dataset_data, **kwargs)
    raise ValueError(f"Unknown engine {engine!r}. Use 'zarr' or 'icechunk'.")


def _open_zarr(dataset_data: dict[str, Any], **kwargs: Any) -> xr.Dataset:
    return xr.open_zarr(dataset_data["zarr_url"], **kwargs)


def _open_icechunk(dataset_data: dict[str, Any], **kwargs: Any) -> xr.Dataset:
    try:
        import icechunk
    except ImportError:
        raise ImportError(
            "The 'icechunk' package is required for engine='icechunk'. "
            "Install it with: pip install dynamical[icechunk]"
        ) from None

    config = dataset_data.get("icechunk")
    if config is None:
        raise ValueError(
            f"Dataset {dataset_data['id']!r} does not have icechunk configuration."
        )

    storage = icechunk.s3_storage(
        bucket=config["bucket"],
        prefix=config["prefix"],
        region=config["region"],
        anonymous=True,
    )
    repo = icechunk.Repository.open(storage)
    session = repo.readonly_session("main")
    store = session.store

    return xr.open_zarr(store, **{"chunks": None, **kwargs})
