from __future__ import annotations

from typing import TYPE_CHECKING, Any

import icechunk
import xarray as xr

if TYPE_CHECKING:
    from zarr.abc.store import Store


def _get_store(dataset_data: dict[str, Any]) -> Store:
    config = dataset_data["icechunk"]
    storage = icechunk.s3_storage(
        bucket=config["bucket"],
        prefix=config["prefix"],
        region=config["region"],
        anonymous=True,
    )
    prefixes = dataset_data.get("virtual_chunk_containers") or []
    authorize = (
        icechunk.containers_credentials(
            {p: icechunk.s3_anonymous_credentials() for p in prefixes}
        )
        if prefixes
        else None
    )
    repo = icechunk.Repository.open(storage, authorize_virtual_chunk_access=authorize)
    session = repo.readonly_session("main")
    return session.store


def _open_dataset(dataset_data: dict[str, Any], **kwargs: Any) -> xr.Dataset:
    # icechunk manages its own metadata; zarr's consolidated metadata doesn't apply.
    kwargs.setdefault("consolidated", False)
    return xr.open_zarr(_get_store(dataset_data), **kwargs)
