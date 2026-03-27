from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import xarray as xr
    import zarr.abc


class DatasetEntry:
    """A single dataset from the dynamical.org catalog."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    @property
    def id(self) -> str:
        return self._data["id"]

    @property
    def name(self) -> str:
        return self._data["name"]

    @property
    def description(self) -> str:
        return self._data["description"]

    @property
    def zarr_url(self) -> str:
        return self._data["zarr_url"]

    @property
    def icechunk_config(self) -> dict[str, str] | None:
        return self._data.get("icechunk")

    @property
    def status(self) -> str:
        return self._data["status"]

    def get_store(self, engine: str = "zarr") -> zarr.abc.Store:
        """Get a zarr Store for this dataset.

        Args:
            engine: "zarr" (default) or "icechunk".

        Returns:
            zarr.abc.Store
        """
        from dynamical._open import _get_store

        return _get_store(self._data, engine=engine)

    def open(self, engine: str = "zarr", **kwargs: Any) -> xr.Dataset:
        """Open this dataset as an xarray Dataset.

        Args:
            engine: "zarr" (default) or "icechunk".
            **kwargs: Passed through to xr.open_zarr().

        Returns:
            xarray.Dataset
        """
        from dynamical._open import _open_dataset

        return _open_dataset(self._data, engine=engine, **kwargs)

    def __repr__(self) -> str:
        return f"DatasetEntry({self.id!r}, {self.name!r})"
