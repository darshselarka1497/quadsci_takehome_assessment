"""Storage abstraction.

Callers never import GCS or S3 clients directly — they only call ``open_uri()``.
The interface is the contract: adding S3 support is a one-function change here,
not a rewrite across the codebase.

Returns a ``(pyarrow.fs.FileSystem, path)`` pair so downstream code can use the
PyArrow dataset API (predicate pushdown / filtered scans) regardless of backend.
"""
from __future__ import annotations

from typing import Tuple
from urllib.parse import urlparse

import pyarrow.fs as pafs


def open_uri(source_uri: str) -> Tuple[pafs.FileSystem, str]:
    """Resolve a ``source_uri`` to a (filesystem, path) pair.

    Supported schemes:
      * ``file://`` (and bare local paths) — local filesystem
      * ``gs://``   — Google Cloud Storage (auth via Workload Identity or
                      GOOGLE_APPLICATION_CREDENTIALS for local dev)
      * ``s3://``   — designed but not implemented (see NotImplementedError below)
    """
    parsed = urlparse(source_uri)
    scheme = parsed.scheme or "file"

    if scheme == "file":
        # urlparse("file:///a/b").path -> "/a/b"; bare paths land in .path too.
        path = parsed.path if parsed.scheme else source_uri
        return pafs.LocalFileSystem(), path

    if scheme == "gs":
        # GcsFileSystem picks up Application Default Credentials / Workload
        # Identity automatically. For local dev, GOOGLE_APPLICATION_CREDENTIALS
        # is honored by the underlying client.
        fs = pafs.GcsFileSystem()
        # PyArrow GCS paths are "bucket/key" (no leading slash).
        path = f"{parsed.netloc}{parsed.path}"
        return fs, path

    if scheme == "s3":
        # To add S3: return pafs.S3FileSystem(), f"{parsed.netloc}{parsed.path}".
        # Left as a documented stub to keep the abstraction's contract explicit —
        # enabling it is a one-function change, not a rewrite.
        raise NotImplementedError(
            "s3:// support is designed but not implemented. Enable by returning "
            "pyarrow.fs.S3FileSystem() here (auth via IAM role / instance profile)."
        )

    raise ValueError(f"Unsupported source_uri scheme: {scheme!r} ({source_uri})")
