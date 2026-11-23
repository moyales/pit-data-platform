"""Raw layer: one immutable, self-describing folder per ingest.

Layout, for each ingest of a source:

    <root>/<source>/<ingest_id>/
        original/<filename>       the download, byte for byte
        content/<content_name>    the extracted payload the table was parsed from
        parsed.parquet            the parsed table, validated against RAW_SCHEMA
        meta.json                 what, where from, when, and the hashes

Raw records only what was observed. Knowledge time is derived later, in the
validated layer, by replaying ingests in order (D-006). There is no shared
manifest: the index of ingests is a scan of the meta.json files (D-008).
"""

import hashlib
import json
import os
import secrets
import shutil
from dataclasses import dataclass
from datetime import datetime
from importlib.metadata import version
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq

# Imported under private names so tests can still patch raw._parquet_bytes.
from pitdata._fs import fsync_dir as _fsync_dir
from pitdata._fs import parquet_bytes as _parquet_bytes
from pitdata._fs import write_durable as _write_durable
from pitdata.schema import RAW_SCHEMA, SCHEMA_VERSION, SchemaError, require_utc, validate

TMP_PREFIX = ".tmp-"
META_FILE = "meta.json"
PARSED_FILE = "parsed.parquet"

# What a parser hands to the writer. The writer alone adds ingest_time and
# ingest_id (D-009), so a parser that emits them is rejected, not overridden.
PARSED_COLUMNS = ("series_id", "valid_date", "value", "source")


@dataclass(frozen=True)
class IngestResult:
    ingest_id: str
    status: Literal["written", "unchanged"]
    path: Path  # the ingest folder (the existing one when unchanged)
    row_count: int


def write_raw(
    root: Path,
    source: str,
    url: str,
    filename: str,
    original: bytes,
    content: bytes,
    content_name: str,
    parsed: pa.Table,
    ingest_time: datetime,
    *,
    parser_version: str,
) -> IngestResult:
    """Write one ingest folder, or return the existing one if `content` was seen before.

    `parser_version` identifies the parser that produced `parsed`. Raw is never
    rewritten, so after a parser fix this is how a reader tells which ingests
    were parsed by the old code (D-012).
    """
    require_utc(ingest_time, "ingest_time")
    _check_plain_name(filename, "filename")
    _check_plain_name(content_name, "content_name")
    if not parser_version:
        raise ValueError("parser_version must be a non-empty string")

    # Hash the extracted content, not the download: a zip stores its members'
    # timestamps, so identical numbers can arrive in different bytes (D-007).
    content_sha256 = hashlib.sha256(content).hexdigest()
    source_dir = root / source
    for meta in list_ingests(root, source):
        if meta["content_sha256"] == content_sha256:
            return IngestResult(
                ingest_id=meta["ingest_id"],
                status="unchanged",
                path=source_dir / meta["ingest_id"],
                row_count=meta["row_count"],
            )

    ingest_id = f"{ingest_time:%Y%m%dT%H%M%SZ}_{content_sha256[:12]}"
    table = validate(_stamp(parsed, source, ingest_time, ingest_id), RAW_SCHEMA)

    meta = {
        "ingest_id": ingest_id,
        "source": source,
        "url": url,
        "filename": filename,
        "content_name": content_name,
        "original_sha256": hashlib.sha256(original).hexdigest(),
        "content_sha256": content_sha256,
        "row_count": table.num_rows,
        "ingest_time": ingest_time.isoformat(),
        "schema_version": SCHEMA_VERSION,
        "pitdata_version": version("pitdata"),
        "parser_version": parser_version,
    }

    # The temp folder lives inside <root>/<source>/ on purpose: os.rename is
    # atomic only within one filesystem, and the rename is what makes an ingest
    # all-or-nothing (D-008).
    final = source_dir / ingest_id
    tmp = source_dir / f"{TMP_PREFIX}{ingest_id}-{secrets.token_hex(4)}"
    source_dir.mkdir(parents=True, exist_ok=True)
    tmp.mkdir()
    try:
        (tmp / "original").mkdir()
        (tmp / "content").mkdir()
        _write_durable(tmp / "original" / filename, original)
        _write_durable(tmp / "content" / content_name, content)
        _write_durable(tmp / PARSED_FILE, _parquet_bytes(table))
        meta_bytes = (json.dumps(meta, indent=2, sort_keys=True) + "\n").encode()
        _write_durable(tmp / META_FILE, meta_bytes)
        for d in (tmp / "original", tmp / "content", tmp):
            _fsync_dir(d)

        # os.rename silently replaces an *empty* target folder on POSIX, so
        # check explicitly. Another writer could still create the target between
        # this check and the rename; v1 assumes one writer at a time.
        if final.exists():
            raise FileExistsError(f"ingest folder already exists: {final}")
        os.rename(tmp, final)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise

    # Make the rename itself durable, not only the files inside the folder.
    _fsync_dir(source_dir)
    return IngestResult(ingest_id=ingest_id, status="written", path=final, row_count=table.num_rows)


def list_ingests(root: Path, source: str) -> list[dict]:
    """Return the meta.json contents of every finished ingest, oldest first.

    Folders starting with `.tmp-` are unfinished writes and are ignored. Any
    other folder without a meta.json is corrupt, and reading it raises.
    """
    source_dir = root / source
    if not source_dir.is_dir():
        return []
    metas = [
        json.loads((d / META_FILE).read_text())
        for d in source_dir.iterdir()
        if d.is_dir() and not d.name.startswith(TMP_PREFIX)
    ]
    return sorted(metas, key=lambda m: (datetime.fromisoformat(m["ingest_time"]), m["ingest_id"]))


def read_ingest(path: Path) -> pa.Table:
    """Read an ingest folder's parsed table, re-validated against RAW_SCHEMA."""
    return validate(pq.read_table(path / PARSED_FILE), RAW_SCHEMA)


def _check_plain_name(name: str, what: str) -> None:
    """Reject anything but a bare file name.

    `content_name` comes from inside a downloaded archive, which is untrusted.
    A name like "../../x" would otherwise write outside the ingest folder.
    """
    if not name or name in {".", ".."} or "/" in name or "\\" in name or "\0" in name:
        raise ValueError(f"{what} must be a plain file name, got {name!r}")


def _stamp(parsed: pa.Table, source: str, ingest_time: datetime, ingest_id: str) -> pa.Table:
    if sorted(parsed.column_names) != sorted(PARSED_COLUMNS):
        raise SchemaError(
            f"parsed table must have exactly {list(PARSED_COLUMNS)}, got {parsed.column_names}"
        )
    # A parser bug must not file rows under another source's folder.
    found = set(parsed.column("source").unique().to_pylist())
    if found - {source}:
        raise SchemaError(f"source column has {sorted(found, key=str)}, expected only {source!r}")
    n = parsed.num_rows
    ts_type = RAW_SCHEMA.field("ingest_time").type
    return parsed.append_column(
        "ingest_time", pa.array([ingest_time] * n, type=ts_type)
    ).append_column("ingest_id", pa.array([ingest_id] * n, type=pa.string()))
