from __future__ import annotations

from typing import TYPE_CHECKING, Any

import xarray as xr
import zarr.storage

if TYPE_CHECKING:
    import zarr.abc


def _get_store(
    dataset_data: dict[str, Any],
    engine: str = "zarr",
) -> zarr.abc.Store:
    if engine == "zarr":
        return zarr.storage.FsspecStore.from_url(dataset_data["zarr_url"])
    if engine == "icechunk":
        return _icechunk_store(dataset_data)
    raise ValueError(f"Unknown engine {engine!r}. Use 'zarr' or 'icechunk'.")


def _open_dataset(
    dataset_data: dict[str, Any],
    engine: str = "zarr",
    **kwargs: Any,
) -> xr.Dataset:
    store = _get_store(dataset_data, engine=engine)
    return xr.open_zarr(store, **kwargs)


def _icechunk_store(dataset_data: dict[str, Any]) -> zarr.abc.Store:
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
    return session.store
