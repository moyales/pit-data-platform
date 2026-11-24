"""Validated layer: derive knowledge time from the raw ingests of one source.

Raw records what each file said and when we fetched it. This module turns that
evidence into records with a `knowledge_time`: when each value could first have
been known. It is a pure function of the raw ingests (no files, no clock), so
the validated layer can be deleted and rebuilt at any time (D-022).

The rules (spec: docs/spec-validated-layer-and-query.md, D-020, D-021):
- A record is created when a (series_id, valid_date) first appears, and whenever
  its value differs from its value in the previous ingest that contained it.
- Records from a source's first ingest get min(lag rule, ingest_time), and are
  "assumed" only when the rule's instant is strictly earlier. All later records
  get their ingest's time and are "observed".
- Values are compared as parsed, then converted (e.g. percent to decimal).
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime

import pyarrow as pa

from pitdata._duckdb import session
from pitdata.schema import RAW_SCHEMA, RECORD_SCHEMA, require_utc, validate


@dataclass(frozen=True)
class LagRule:
    """Assumed knowledge time for backfilled history.

    The instant is 00:00 UTC on the first day of the month that comes
    `months_after + 1` months after the valid month. In words: the value is
    assumed known by the end of the `months_after`-th month after the month it
    describes. Named and versioned so a change to the rule is visible (D-021).
    """

    name: str
    version: str
    months_after: int

    def __post_init__(self) -> None:
        if self.months_after < 0:
            raise ValueError("months_after must be >= 0")

    def instant(self, valid_date: date) -> datetime:
        months = valid_date.year * 12 + (valid_date.month - 1) + self.months_after + 1
        return datetime(months // 12, months % 12 + 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class SourceConfig:
    """What the validated layer needs to know about one source (D-026).

    Declared in the source's own module, so this module stays generic.
    """

    lag_rule: LagRule
    # Stored value = value as parsed / divide_by. Dividing by an exact number
    # (100) rounds once; multiplying by 0.01, which binary cannot represent
    # exactly, would round twice.
    divide_by: float = 1.0

    def __post_init__(self) -> None:
        if not self.divide_by > 0:
            raise ValueError("divide_by must be positive")


# One query states the whole derivation rule (D-025). The window looks back over
# the ingests that contain each key, so an ingest that is missing the key is
# skipped, exactly as the spec's rule requires.
_DERIVE_SQL = """
WITH ordered AS (
    SELECT
        *,
        row_number() OVER w AS nth,
        lag(value) OVER w AS prev_value
    FROM raw
    WINDOW w AS (PARTITION BY series_id, valid_date ORDER BY ingest_time)
),
changes AS (
    SELECT
        *,
        -- The cast to TIMESTAMPTZ uses the session time zone, which is why
        -- derive_records sets it to UTC before running this.
        CAST(date_trunc('month', valid_date) + to_months($months) AS TIMESTAMPTZ) AS rule_time
    FROM ordered
    -- nth = 1 is not redundant: for a key whose first value is null, lag() is
    -- also null, and NULL IS DISTINCT FROM NULL is false.
    WHERE nth = 1 OR value IS DISTINCT FROM prev_value
)
SELECT
    series_id,
    valid_date,
    value / $divide_by AS value,
    CASE WHEN from_first_ingest AND rule_time < ingest_time
        THEN rule_time ELSE ingest_time END AS knowledge_time,
    CASE WHEN from_first_ingest AND rule_time < ingest_time
        THEN 'assumed' ELSE 'observed' END AS knowledge_basis,
    ingest_time,
    ingest_id,
    source
FROM changes
ORDER BY series_id, valid_date, knowledge_time
"""

_DUPLICATE_KEYS_SQL = """
SELECT ingest_id, series_id, valid_date, count(*) AS n
FROM raw
GROUP BY ingest_id, series_id, valid_date
HAVING count(*) > 1
ORDER BY ingest_id, series_id, valid_date
LIMIT 5
"""


def derive_records(ingests: Sequence[tuple[dict, pa.Table]], config: SourceConfig) -> pa.Table:
    """Derive validated records from every raw ingest of one source.

    `ingests` are (meta.json contents, raw table) pairs, in any order: they are
    sorted by ingest_time here. Returns a table matching RECORD_SCHEMA, sorted by
    series_id, valid_date, knowledge_time. The same input always gives the same
    output.
    """
    if not ingests:
        return RECORD_SCHEMA.empty_table()

    tables = _check_and_tag(ingests)

    # A private UTC session (D-031): the lag rule's instant is a DATE cast to
    # TIMESTAMPTZ, which would otherwise use the machine's local zone (D-024).
    with session() as con:
        con.register("raw", pa.concat_tables(tables))
        duplicates = con.sql(_DUPLICATE_KEYS_SQL).fetchall()
        if duplicates:
            raise ValueError(f"an ingest has the same series and date more than once: {duplicates}")
        result = con.sql(
            _DERIVE_SQL,
            params={"months": config.lag_rule.months_after + 1, "divide_by": config.divide_by},
        ).to_arrow_table()

    return validate(result, RECORD_SCHEMA)


def _check_and_tag(ingests: Sequence[tuple[dict, pa.Table]]) -> list[pa.Table]:
    """Validate the input, and mark the rows that come from the first ingest.

    The first ingest is found from the metadata, not from the rows, so it is
    right even when that ingest has no rows.
    """
    sources = {meta["source"] for meta, _ in ingests}
    if len(sources) != 1:
        raise ValueError(f"ingests must all be from one source, got {sorted(sources)}")

    times = [_ingest_time(meta) for meta, _ in ingests]
    if len(set(times)) != len(times):
        raise ValueError("two ingests have the same ingest_time; their order is undefined")
    first = min(times)

    tagged = []
    for (meta, table), time in zip(ingests, times, strict=True):
        table = validate(table, RAW_SCHEMA)
        ids = set(table.column("ingest_id").to_pylist())
        stamps = set(table.column("ingest_time").to_pylist())
        if ids - {meta["ingest_id"]} or stamps - {time}:
            raise ValueError(f"ingest {meta['ingest_id']}: rows do not match its meta.json")
        if set(table.column("source").to_pylist()) - {meta["source"]}:
            raise ValueError(f"ingest {meta['ingest_id']}: rows from another source")
        flag = pa.array([time == first] * table.num_rows, type=pa.bool_())
        tagged.append(table.append_column("from_first_ingest", flag))
    return tagged


def _ingest_time(meta: dict) -> datetime:
    time = datetime.fromisoformat(meta["ingest_time"])
    return require_utc(time, f"ingest {meta['ingest_id']}: ingest_time")
