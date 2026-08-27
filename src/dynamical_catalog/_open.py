from __future__ import annotations

from typing import TYPE_CHECKING, Any

import icechunk
import xarray as xr

from dynamical_catalog.exceptions import DatasetOpenError

if TYPE_CHECKING:
    from zarr.abc.store import Store


def _build_storage(config: dict[str, Any]) -> icechunk.Storage:
    """Build read-only, unauthenticated icechunk storage for a repository.

    Covers every icechunk backend that can be read without credentials. The
    parsed config's ``type`` is set by the asset href scheme; see
    :func:`dynamical_catalog._stac._parse_icechunk_asset`.
    """
    storage_type = config["type"]
    if storage_type == "s3":
        return icechunk.s3_storage(
            bucket=config["bucket"],
            prefix=config["prefix"],
            region=config["region"],
            anonymous=True,
        )
    if storage_type == "gcs":
        return icechunk.gcs_storage(
            bucket=config["bucket"],
            prefix=config["prefix"],
            anonymous=True,
        )
    if storage_type == "azure":
        return icechunk.azure_storage(
            account=config["account"],
            container=config["container"],
            prefix=config["prefix"],
            anonymous=True,
        )
    if storage_type == "tigris":
        return icechunk.tigris_storage(
            bucket=config["bucket"],
            prefix=config["prefix"],
            region=config["region"],
            anonymous=True,
        )
    if storage_type == "http":
        return icechunk.http_storage(base_url=config["base_url"])
    raise ValueError(f"Unsupported icechunk storage type {storage_type!r}")


def _container_credentials(container: dict[str, str]) -> Any:
    """Build no-auth credentials for one virtual chunk container.

    Tigris shares S3's anonymous credential: icechunk treats `tigris://` as
    part of the S3 family of stores.
    """
    container_type = container["type"]
    if container_type in ("s3", "tigris"):
        return icechunk.s3_anonymous_credentials()
    if container_type == "gcs":
        return icechunk.gcs_credentials(anonymous=True)
    if container_type == "azure":
        return icechunk.azure_anonymous_credentials()
    if container_type == "http":
        return icechunk.Credentials.HttpAccess()
    raise ValueError(f"Unsupported virtual chunk container type {container_type!r}")


def _get_repository(dataset_data: dict[str, Any]) -> icechunk.Repository:
    config = dataset_data["icechunk"]
    storage = _build_storage(config)
    # A virtual dataset's chunks are byte-range references into public source
    # buckets; icechunk raises unless each such container is authorized at open
    # time (the container registration itself is already persisted on the repo).
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
        return icechunk.Repository.open(
            storage, authorize_virtual_chunk_access=authorize
        )
    except icechunk.IcechunkError as e:
        raise DatasetOpenError(
            f"Failed to open icechunk repository for dataset {dataset_id!r}: {e}",
            dataset_id=dataset_id,
        ) from e


def _get_store(dataset_data: dict[str, Any]) -> Store:
    repo = _get_repository(dataset_data)
    dataset_id = dataset_data.get("id")
    try:
        return repo.readonly_session("main").store
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
