"""The as-of query: what was known about these series at time T?

For each (series_id, valid_date) in range, return the record with the greatest
knowledge_time <= asof (D-024). That one rule is the point of the project.

`get` reads the validated Parquet files under `root`; `get_from` runs on an
in-memory table (the property tests use it). Both run the same SQL text, so
what the tests check is what real queries execute (D-029).
"""

from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from pitdata.schema import RECORD_SCHEMA, require_utc, validate

DEFAULT_ROOT = Path("data/validated")

# Columns and types of every result, including an empty one.
RESULT_SCHEMA = pa.schema(
    [
        RECORD_SCHEMA.field(name)
        for name in (
            "series_id",
            "valid_date",
            "value",
            "knowledge_time",
            "knowledge_basis",
            "ingest_id",
        )
    ]
)

_AS_OF_SQL = """
SELECT series_id, valid_date, value, knowledge_time, knowledge_basis, ingest_id
FROM records
-- WHERE runs before QUALIFY, so what was not yet known is removed first, and
-- only then is the latest remaining record picked. The other order would pick
-- a restatement made after asof, then filter it out, and lose the key entirely.
WHERE knowledge_time <= $asof
  AND list_contains($series, series_id)
  AND valid_date BETWEEN $start AND $end
-- row_number() picks a whole row, so a null latest value is returned as null.
-- An aggregate such as arg_max(value, knowledge_time) can skip nulls and
-- return an older, stale value instead.
QUALIFY row_number() OVER (
    PARTITION BY series_id, valid_date ORDER BY knowledge_time DESC
) = 1
ORDER BY series_id, valid_date
"""

# Two records for one key at the same knowledge time make "the latest"
# ambiguous: the answer would depend on the order rows happen to be read in.
_TIES_SQL = """
-- Text, not a timestamp: fetchall() converts TIMESTAMPTZ values to Python with
-- pytz, which is not a dependency. Only this error message needs the values.
SELECT series_id, CAST(valid_date AS VARCHAR), CAST(knowledge_time AS VARCHAR)
FROM records
WHERE list_contains($series, series_id) AND valid_date BETWEEN $start AND $end
GROUP BY series_id, valid_date, knowledge_time
HAVING count(*) > 1
ORDER BY ALL
LIMIT 5
"""

# For get(): the same queries, with `records` defined over the files.
_FROM_FILES = "WITH records AS (SELECT * FROM read_parquet($files))\n"


class QueryError(ValueError):
    """Raised when there is nothing to query or the records are ambiguous."""


def get(
    series: str | Sequence[str],
    start: date,
    end: date,
    asof: datetime,
    *,
    root: Path = DEFAULT_ROOT,
) -> pa.Table:
    """As-of query over the validated files under `root` (written by `pit build`).

    `start` and `end` are inclusive dates. `asof` must be an aware UTC datetime.
    Returns RESULT_SCHEMA columns, sorted by series_id, valid_date. A series that
    is unknown, or not yet known at `asof`, has no rows.
    """
    series_list = _check_inputs(series, start, end, asof)
    files = _validated_files(root)
    params = {"files": [str(f) for f in files]}
    return _run(_FROM_FILES, params, series_list, start, end, asof, register=None)


def get_from(
    records: pa.Table,
    series: str | Sequence[str],
    start: date,
    end: date,
    asof: datetime,
) -> pa.Table:
    """The same as-of query, on an in-memory table of records."""
    series_list = _check_inputs(series, start, end, asof)
    records = validate(records, RECORD_SCHEMA)
    return _run("", {}, series_list, start, end, asof, register=records)


def _run(
    prefix: str,
    params: dict,
    series: list[str],
    start: date,
    end: date,
    asof: datetime,
    register: pa.Table | None,
) -> pa.Table:
    # A private connection: TimeZone must not leak to or from other DuckDB users.
    con = duckdb.connect()
    try:
        # Defensive, not load-bearing today: every comparison here is instant vs
        # instant or date vs date, and the final cast labels the timestamps UTC.
        # It protects any future expression that mixes dates and timestamps,
        # which would otherwise use the machine's local zone (see derive_records).
        con.execute("SET TimeZone = 'UTC'")
        if register is not None:
            con.register("records", register)
        scope = {**params, "series": series, "start": start, "end": end}
        ties = con.sql(prefix + _TIES_SQL, params=scope).fetchall()
        if ties:
            raise QueryError(f"records with the same key and knowledge_time: {ties}")
        result = con.sql(prefix + _AS_OF_SQL, params={**scope, "asof": asof}).to_arrow_table()
    finally:
        con.close()
    # The cast is what guarantees the result's types, including the UTC label on
    # knowledge_time (relabelling a zone keeps the same instant).
    return result.cast(RESULT_SCHEMA)


def _check_inputs(series: str | Sequence[str], start: date, end: date, asof: datetime) -> list[str]:
    series_list = [series] if isinstance(series, str) else list(series)
    if not series_list or not all(isinstance(s, str) for s in series_list):
        raise ValueError(f"series must be a string or a non-empty list of strings, got {series!r}")
    for name, value in (("start", start), ("end", end)):
        # datetime is a subclass of date, so isinstance(value, date) alone would
        # accept a datetime here.
        if isinstance(value, datetime) or not isinstance(value, date):
            raise TypeError(f"{name} must be a date (not a datetime), got {value!r}")
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    require_utc(asof, "asof")
    return series_list


def _validated_files(root: Path) -> list[Path]:
    """The validated files under `root`, each schema-checked from metadata only.

    Listed in Python, skipping hidden names, so a temp file left by a crashed
    build (".tmp-...") is never read.
    """
    files = (
        sorted(
            p
            for p in root.iterdir()
            if p.is_file() and p.suffix == ".parquet" and not p.name.startswith(".")
        )
        if root.is_dir()
        else []
    )
    if not files:
        raise QueryError(f"no validated files under {root}; run `pit build` first")
    for path in files:
        # Reads only the file footer: an empty table with the file's schema is
        # enough for validate() to check columns, types and time zones.
        try:
            validate(pq.read_schema(path).empty_table(), RECORD_SCHEMA)
        except ValueError as e:
            raise QueryError(f"{path}: not a validated records file: {e}") from e
    return files
