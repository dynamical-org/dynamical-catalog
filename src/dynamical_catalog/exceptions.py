"""Exception hierarchy raised by ``dynamical_catalog``.

All exceptions raised by this package inherit from
:class:`DynamicalCatalogError`, so callers can write a single
``except DynamicalCatalogError`` to catch anything we raise.
"""

from __future__ import annotations


class DynamicalCatalogError(Exception):
    """Base class for all exceptions raised by ``dynamical_catalog``."""


class UnknownDatasetError(DynamicalCatalogError, ValueError):
    """Raised when a user-supplied ``dataset_id`` is not in the catalog.

    Multi-inherits from :class:`ValueError` so callers that previously
    caught ``ValueError`` keep working.
    """


class CatalogFetchError(DynamicalCatalogError):
    """Raised when fetching the STAC catalog fails.

    Covers network errors, HTTP errors, and malformed JSON responses.

    Attributes:
        url: The URL (or list of URLs, joined) that failed.
        attempts: Number of attempts made before giving up.
    """

    def __init__(
        self,
        message: str,
        *,
        url: str,
        attempts: int,
    ) -> None:
        super().__init__(message)
        self.url = url
        self.attempts = attempts


class InvalidCatalogError(DynamicalCatalogError):
    """Raised when the STAC catalog response is reachable but malformed."""


class DatasetOpenError(DynamicalCatalogError):
    """Raised when opening the icechunk repo or zarr/xarray dataset fails.

    Covers eager errors at open time (e.g. bucket not found, virtual chunk
    authorization). Lazy errors raised when the caller later reads data
    from the returned :class:`xarray.Dataset` are not wrapped.
    """


__all__ = [
    "CatalogFetchError",
    "DatasetOpenError",
    "DynamicalCatalogError",
    "InvalidCatalogError",
    "UnknownDatasetError",
]
