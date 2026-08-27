from __future__ import annotations

from typing import TYPE_CHECKING, Any

import icechunk
import xarray as xr

from dynamical_catalog.exceptions import DatasetOpenError

if TYPE_CHECKING:
    from zarr.abc.store import Store


def _build_storage(config: dict[str, Any]) -> icechunk.Storage:
    """Build read-only, unauthenticated icechunk storage for a repository."""
    storage_type = config["type"]
    if storage_type == "s3":
        return icechunk.s3_storage(
            bucket=config["bucket"],
            prefix=config["prefix"],
            region=config["region"],
            anonymous=True,
        )
    if storage_type == "http":
        return icechunk.http_storage(base_url=config["base_url"])
    raise ValueError(f"Unsupported icechunk storage type {storage_type!r}")


def _container_credentials(container: dict[str, str]) -> Any:
    container_type = container["type"]
    if container_type == "s3":
        return icechunk.s3_anonymous_credentials()
    if container_type == "http":
        return icechunk.Credentials.HttpAccess()
    raise ValueError(f"Unsupported virtual chunk container type {container_type!r}")


def _get_store(dataset_data: dict[str, Any]) -> Store:
    config = dataset_data["icechunk"]
    storage = _build_storage(config)
    containers = dataset_data.get("virtual_chunk_containers") or []
    authorize = (
        icechunk.containers_credentials(
            {c["url_prefix"]: _container_credentials(c) for c in containers}
        )
        if containers
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
