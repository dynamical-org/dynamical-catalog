from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dynamical._dataset_entry import DatasetEntry


class Catalog:
    """Provides attribute-style access to dynamical.org datasets.

    Supports tab-completion in IPython/Jupyter::

        >>> import dynamical
        >>> dynamical.catalog.<TAB>
        >>> dynamical.catalog.noaa_gfs_forecast.open()
    """

    def __init__(self, loader: Callable[[], dict[str, dict[str, Any]]]) -> None:
        self._loader = loader
        self._entries: dict[str, DatasetEntry] = {}

    @property
    def _datasets(self) -> dict[str, dict[str, Any]]:
        return self._loader()

    def __getattr__(self, name: str) -> DatasetEntry:
        if name.startswith("_"):
            raise AttributeError(name)
        dataset_id = name.replace("_", "-")
        datasets = self._datasets
        if dataset_id not in datasets:
            available = ", ".join(self._dataset_attr_names())
            raise AttributeError(f"No dataset named {name!r}. Available: {available}")
        if dataset_id not in self._entries:
            self._entries[dataset_id] = DatasetEntry(datasets[dataset_id])
        return self._entries[dataset_id]

    def __dir__(self) -> list[str]:
        return list(self._dataset_attr_names()) + list(super().__dir__())

    def _dataset_attr_names(self) -> list[str]:
        return [did.replace("-", "_") for did in self._datasets]

    def __repr__(self) -> str:
        names = self._dataset_attr_names()
        return f"Catalog({len(names)} datasets: {', '.join(names)})"
