from __future__ import annotations

from typing import TYPE_CHECKING, Any

import icechunk
import xarray as xr
import zarr.storage

if TYPE_CHECKING:
    from zarr.abc.store import Store


def _get_store(
    dataset_data: dict[str, Any],
    engine: str = "icechunk",
) -> Store:
    if engine == "icechunk":
        if dataset_data.get("icechunk") is not None:
            return _icechunk_store(dataset_data)
        return zarr.storage.FsspecStore.from_url(dataset_data["zarr_url"])
    if engine == "zarr":
        return zarr.storage.FsspecStore.from_url(dataset_data["zarr_url"])
    raise ValueError(f"Unknown engine {engine!r}. Use 'icechunk' or 'zarr'.")


def _open_dataset(
    dataset_data: dict[str, Any],
    engine: str = "icechunk",
    **kwargs: Any,
) -> xr.Dataset:
    store = _get_store(dataset_data, engine=engine)
    return xr.open_zarr(store, **kwargs)


def _icechunk_store(dataset_data: dict[str, Any]) -> Store:
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
