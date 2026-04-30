from __future__ import annotations

from typing import TYPE_CHECKING, Any

import icechunk
import xarray as xr

from dynamical_catalog.exceptions import DatasetOpenError

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
    dataset_id = dataset_data.get("id")
    try:
        repo = icechunk.Repository.open(
            storage, authorize_virtual_chunk_access=authorize
        )
        session = repo.readonly_session("main")
        return session.store
    except icechunk.IcechunkError as e:
        raise DatasetOpenError(
            f"Failed to open icechunk repository for dataset {dataset_id!r}: {e}",
            dataset_id=dataset_id,
        ) from e


def _open_dataset(dataset_data: dict[str, Any], **kwargs: Any) -> xr.Dataset:
    # icechunk manages its own metadata; zarr's consolidated metadata doesn't apply.
    kwargs.setdefault("consolidated", False)
    dataset_id = dataset_data.get("id")
    store = _get_store(dataset_data)
    try:
        return xr.open_zarr(store, **kwargs)
    except Exception as e:
        raise DatasetOpenError(
            f"Failed to open dataset {dataset_id!r} as xarray Dataset: {e}",
            dataset_id=dataset_id,
        ) from e
