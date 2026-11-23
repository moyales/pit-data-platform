"""Durable file writes, shared by the raw writer and the validated build.

Both layers write files into a temporary location and then move them into
place in one step (a rename for raw ingest folders, a replace for validated
files). The move is atomic, but atomic is about what readers *see*, not about
what survives a power loss. These helpers make sure the bytes are on disk
before the move publishes them (D-013).
"""

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def parquet_bytes(table: pa.Table) -> bytes:
    # Serialise in memory so every file goes through the same durable write.
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


def write_durable(path: Path, data: bytes) -> None:
    """Create `path` (it must not exist), write `data` and fsync it.

    Without the fsync, a power loss can leave a complete-looking file, already
    moved into place, that holds truncated data.
    """
    with open(path, "xb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())


def fsync_dir(path: Path) -> None:
    # Flushes the folder's list of entries (new files, a rename) to disk.
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
