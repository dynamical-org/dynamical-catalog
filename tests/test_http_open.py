"""Serve a real icechunk repo over local HTTP and open it through _open.

Covers the HTTPS-backed code path — ``icechunk.http_storage`` for the
repository and ``Credentials.HttpAccess`` for virtual chunk containers — the
way a bucket published on a custom domain is read. Nothing here touches the
network: a threaded HTTP server on loopback stands in for the bucket, so the
test also serves as a check that the path works without any object-store
credentials at all.

The catalog only admits ``https://`` hrefs (see test_stac.py); loopback is
plain HTTP, so these tests drive ``_get_store``/``_open_dataset`` directly
with the config ``_parse_collection`` produces.
"""

from __future__ import annotations

import functools
import re
import threading
from collections.abc import Iterator
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, NamedTuple

import icechunk
import numpy as np
import pytest
import xarray as xr
import zarr

from dynamical_catalog._open import _get_store, _open_dataset

_REPO_DIR = "repo.icechunk"
_CHUNK_FILE = "chunks/values.bin"
_VALUES = np.arange(8, dtype="int32")


class _Served(NamedTuple):
    base_url: str
    container_prefix: str


class _RangeHandler(SimpleHTTPRequestHandler):
    """A static file handler that honours single-range GETs.

    icechunk fetches manifests with a Range header, which
    SimpleHTTPRequestHandler answers with the whole body — so the stock
    handler fails as "Range request not supported". Object stores serving
    a real repository support ranges, so the stand-in has to as well.
    """

    def log_message(self, *args: Any) -> None:
        """Keep pytest output clean; the default handler logs every request."""

    def do_GET(self) -> None:
        self._respond(include_body=True)

    def do_HEAD(self) -> None:
        self._respond(include_body=False)

    def _respond(self, *, include_body: bool) -> None:
        try:
            data = Path(self.translate_path(self.path)).read_bytes()
        except OSError:
            self.send_error(404)
            return

        body, status, content_range = data, 200, None
        requested = self.headers.get("Range")
        if requested is not None:
            resolved = _resolve_range(requested, len(data))
            if resolved is None:
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{len(data)}")
                self.end_headers()
                return
            start, end = resolved
            body, status = data[start : end + 1], 206
            content_range = f"bytes {start}-{end}/{len(data)}"

        self.send_response(status)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Accept-Ranges", "bytes")
        if content_range is not None:
            self.send_header("Content-Range", content_range)
        self.end_headers()
        if include_body:
            self.wfile.write(body)


def _resolve_range(header: str, size: int) -> tuple[int, int] | None:
    """Resolve a single `bytes=` range against a body size, or None if unsatisfiable."""
    match = re.fullmatch(r"bytes=(\d*)-(\d*)", header.strip())
    if match is None:
        return None
    first, last = match.groups()
    if not first and not last:
        return None
    if not first:  # suffix form: the final N bytes
        length = int(last)
        return (max(0, size - length), size - 1) if length else None
    start = int(first)
    end = int(last) if last else size - 1
    end = min(end, size - 1)
    return (start, end) if start <= end else None


@pytest.fixture
def served_repo(tmp_path: Path) -> Iterator[_Served]:
    """Serve an icechunk repo, and the bytes its virtual chunk points at, over HTTP.

    The port is only known once the server is bound, and the virtual chunk
    container prefix has to be baked into the repo at creation time, so the
    server starts first and the repo is built against its address.
    """
    doc_root = tmp_path / "www"
    (doc_root / "chunks").mkdir(parents=True)
    (doc_root / _CHUNK_FILE).write_bytes(_VALUES.tobytes())

    handler = functools.partial(_RangeHandler, directory=str(doc_root))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    host, port = server.server_address[:2]
    origin = f"http://{host}:{port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        _build_repo(doc_root / _REPO_DIR, f"{origin}/chunks/")
        yield _Served(
            base_url=f"{origin}/{_REPO_DIR}",
            container_prefix=f"{origin}/chunks/",
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _build_repo(path: Path, container_prefix: str) -> None:
    """Create a repo on disk holding one virtual chunk served over HTTP."""
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            url_prefix=container_prefix, store=icechunk.http_store()
        )
    )
    repo = icechunk.Repository.create(
        icechunk.local_filesystem_storage(str(path)),
        config=config,
        authorize_virtual_chunk_access=icechunk.containers_credentials(
            {container_prefix: icechunk.Credentials.HttpAccess()}
        ),
    )
    session = repo.writable_session("main")
    root = zarr.create_group(store=session.store)
    root.create_array(
        name="values",
        shape=_VALUES.shape,
        chunks=_VALUES.shape,
        dtype=_VALUES.dtype,
        compressors=None,
        dimension_names=("i",),
    )
    session.store.set_virtual_ref(
        "values/c/0",
        f"{container_prefix}values.bin",
        offset=0,
        length=_VALUES.nbytes,
    )
    session.commit("add virtual ref served over http")


def _dataset_config(served: _Served, *, with_container: bool = True) -> dict[str, Any]:
    config: dict[str, Any] = {
        "id": "http-test",
        "icechunk": {"type": "http", "base_url": served.base_url},
    }
    if with_container:
        config["virtual_chunk_containers"] = [
            {"url_prefix": served.container_prefix, "type": "http"}
        ]
    return config


class TestHttpBackedRepository:
    def test_get_store_yields_a_readable_zarr_store(self, served_repo: _Served) -> None:
        store = _get_store(_dataset_config(served_repo))
        assert "values" in zarr.open_group(store=store, mode="r")

    def test_reads_through_an_http_virtual_chunk(self, served_repo: _Served) -> None:
        ds = _open_dataset(_dataset_config(served_repo))
        assert isinstance(ds, xr.Dataset)
        np.testing.assert_array_equal(ds["values"].values, _VALUES)

    def test_unauthorized_container_blocks_read(self, served_repo: _Served) -> None:
        ds = _open_dataset(_dataset_config(served_repo, with_container=False))
        # Lazy reads are out of scope for the DatasetOpenError wrap, so the
        # underlying icechunk error still surfaces here.
        with pytest.raises(icechunk.IcechunkError, match="virtual chunk"):
            _ = ds["values"].values
