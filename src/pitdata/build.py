"""Rebuild the validated layer from raw: one Parquet file per source.

The validated layer is derived data (D-022). Each build reads every finished
raw ingest of a source, runs `derive_records`, and replaces
`<out>/<source>.parquet` in one atomic step. Raw is only read, never written.

What produced each file (lag rule, conversion, raw ingest ids, pitdata
version) is stored inside the file's own Parquet metadata, so it can never be
separated from the data it describes (D-027).
"""

import json
import os
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from pitdata._fs import fsync_dir, parquet_bytes, write_durable
from pitdata.raw import TMP_PREFIX, list_ingests, read_ingest
from pitdata.schema import RECORD_SCHEMA, validate
from pitdata.validated import SourceConfig, derive_records

BUILD_INFO_KEY = b"pitdata_build"


class BuildError(ValueError):
    """Raised when the raw folders and the known sources do not match."""


@dataclass(frozen=True)
class BuildResult:
    source: str
    path: Path
    record_count: int
    restated_keys: int  # keys with more than one record
    ingest_count: int


def build(
    raw_root: Path,
    out_root: Path,
    configs: Mapping[str, SourceConfig],
    only: str | None = None,
) -> list[BuildResult]:
    """Build every source under `raw_root`, or just `only`.

    Checks all sources before writing anything, so a raw folder with no config
    stops the build with nothing changed (D-028).
    """
    found = _raw_sources(raw_root)
    if only is not None:
        if only not in configs:
            raise BuildError(f"unknown source {only!r}; known sources: {sorted(configs)}")
        if only not in found:
            raise BuildError(f"no raw data for source {only!r} under {raw_root}")
        sources = [only]
    else:
        unknown = sorted(set(found) - set(configs))
        if unknown:
            raise BuildError(
                f"raw folders with no registered config: {unknown}. "
                "Register them in pitdata/sources/__init__.py, or build one source with --source."
            )
        sources = sorted(found)

    return [build_source(raw_root, out_root, s, configs[s]) for s in sources]


def build_source(raw_root: Path, out_root: Path, source: str, config: SourceConfig) -> BuildResult:
    """Derive one source and atomically replace its validated file."""
    metas = list_ingests(raw_root, source)
    ingests = [(m, read_ingest(raw_root / source / m["ingest_id"])) for m in metas]
    records = derive_records(ingests, config)
    restated = _restated_keys(records)

    info = {
        "source": source,
        "lag_rule": {
            "name": config.lag_rule.name,
            "version": config.lag_rule.version,
            "months_after": config.lag_rule.months_after,
        },
        "divide_by": config.divide_by,
        # In ingest_time order (list_ingests sorts them). Together with the rule
        # and the pitdata version, these fully determine the file's content, so
        # no build time is recorded: builds stay deterministic.
        "raw_ingest_ids": [m["ingest_id"] for m in metas],
        "pitdata_version": version("pitdata"),
        "record_count": records.num_rows,
        "restated_keys": restated,
    }
    metadata = {**(records.schema.metadata or {}), BUILD_INFO_KEY: _json_bytes(info)}
    data = parquet_bytes(records.replace_schema_metadata(metadata))

    out_root.mkdir(parents=True, exist_ok=True)
    final = out_root / f"{source}.parquet"
    # Same folder as the final file: os.replace is atomic only within one
    # filesystem. Readers open <source>.parquet by exact name, so a temp file
    # left by a crash is never read.
    tmp = out_root / f"{TMP_PREFIX}{source}-{secrets.token_hex(4)}.parquet"
    try:
        write_durable(tmp, data)
        # Unlike the raw writer's rename, replacing the old file is the point:
        # readers see the previous complete file or the new one, never a mix.
        os.replace(tmp, final)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    fsync_dir(out_root)

    return BuildResult(
        source=source,
        path=final,
        record_count=records.num_rows,
        restated_keys=restated,
        ingest_count=len(metas),
    )


def read_validated(path: Path) -> pa.Table:
    """Read a validated file, re-validated against RECORD_SCHEMA."""
    return validate(pq.read_table(path), RECORD_SCHEMA)


def read_build_info(path: Path) -> dict:
    """Return what produced a validated file (rule, conversion, raw ingest ids, version)."""
    metadata = pq.read_schema(path).metadata or {}
    if BUILD_INFO_KEY not in metadata:
        raise BuildError(f"{path}: no build information; was it written by pit build?")
    return json.loads(metadata[BUILD_INFO_KEY])


def _raw_sources(raw_root: Path) -> list[str]:
    # Each folder under raw is a source. Hidden entries (".DS_Store") are not.
    if not raw_root.is_dir():
        return []
    return sorted(p.name for p in raw_root.iterdir() if p.is_dir() and not p.name.startswith("."))


def _restated_keys(records: pa.Table) -> int:
    if records.num_rows == 0:
        return 0
    counts = records.group_by(["series_id", "valid_date"]).aggregate([("series_id", "count")])
    return pc.sum(pc.greater(counts.column("series_id_count"), 1)).as_py() or 0


def _json_bytes(obj: dict) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()
