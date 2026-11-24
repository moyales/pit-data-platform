"""Quality checks on validated records: they warn, they never stop a build (D-023, D-034).

Schema violations still raise in `validate`. These checks look for data that
is well-formed but suspicious, and report it in `<source>.quality.json`.

Which data each check looks at:
- Value checks (nulls, outliers) cover every record, not only the latest per
  key. A bad value that was later corrected was still visible at some T, so a
  backtest could have used it.
- Date checks (calendar, gaps) cover the set of (series_id, valid_date) keys,
  which is the same at every T after the first ingest.

Each check reports a status, a count and up to EXAMPLES examples. Statuses:
`ok`, `warn`, `info` (reported for reading, never warns) and `skipped` (not
configured for this source).
"""

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import date

import pyarrow as pa

from pitdata import calendars
from pitdata._duckdb import session
from pitdata.schema import RECORD_SCHEMA, validate

EXAMPLES = 20

SessionsFn = Callable[[str, date, date], frozenset[date]]


@dataclass(frozen=True)
class QualityConfig:
    """What to check for one source. Kept apart from SourceConfig, which
    determines the records and is recorded in each validated file (D-027);
    changing a reporting threshold must not change that provenance."""

    # A name from calendars.known_calendars(), or None for series that do not
    # follow an exchange calendar (for example monthly macro data).
    calendar: str | None = None
    # Converted values with an absolute value above this are outliers. None
    # turns the check off.
    max_abs_value: float | None = None
    # An ingest that restates more than this fraction of the rows it contains
    # suggests the source re-ran its history.
    restatement_warn_fraction: float = 0.01

    def __post_init__(self) -> None:
        if self.calendar is not None and self.calendar not in calendars.known_calendars():
            raise ValueError(f"unknown calendar {self.calendar!r}")
        if self.max_abs_value is not None and not self.max_abs_value > 0:
            raise ValueError("max_abs_value must be positive")
        if not 0 <= self.restatement_warn_fraction <= 1:
            raise ValueError("restatement_warn_fraction must be between 0 and 1")

    def to_dict(self) -> dict:
        return asdict(self)


def check_quality(
    records: pa.Table,
    config: QualityConfig,
    ingest_rows: Mapping[str, int],
    sessions: SessionsFn = calendars.sessions,
) -> dict:
    """Run every check on one source's records.

    `ingest_rows` maps each raw ingest id to its number of rows, for the
    restatement fractions. `sessions` is injected so tests need no calendar.
    Returns {check name: result}; the same input always gives the same output.
    """
    records = validate(records, RECORD_SCHEMA)
    with session() as con:
        con.register("records", records)
        checks = {
            **_calendar_checks(con, config, sessions),
            "restatements": _restatements(con, config, ingest_rows),
            "nulls": _nulls(con),
            "outliers": _outliers(con, config),
            "gaps": _gaps(con),
        }
    return checks


def warnings(checks: Mapping[str, dict]) -> list[str]:
    """Names of the checks that warn, with their counts, e.g. 'nulls (3)'."""
    return [f"{name} ({c['count']:,})" for name, c in checks.items() if c["status"] == "warn"]


def _result(rows: list, *, warn_if_any: bool = True, **extra) -> dict:
    return {
        "status": "warn" if (warn_if_any and rows) else "ok",
        "count": len(rows),
        "examples": rows[:EXAMPLES],
        **extra,
    }


def _skipped(reason: str) -> dict:
    return {"status": "skipped", "count": 0, "examples": [], "reason": reason}


def _calendar_checks(con, config: QualityConfig, sessions: SessionsFn) -> dict:
    if config.calendar is None:
        reason = "no calendar configured for this source"
        return {"dates_not_sessions": _skipped(reason), "sessions_missing": _skipped(reason)}
    lo, hi = con.sql("SELECT min(valid_date), max(valid_date) FROM records").fetchall()[0]
    if lo is None:
        return {"dates_not_sessions": _result([]), "sessions_missing": _result([])}

    days = sorted(sessions(config.calendar, lo, hi))
    con.register("sessions", pa.table({"day": pa.array(days, type=pa.date32())}))

    not_sessions = con.sql("""
        SELECT DISTINCT series_id, valid_date FROM records
        WHERE valid_date NOT IN (SELECT day FROM sessions)
        ORDER BY valid_date, series_id
    """).fetchall()
    # Per series, within that series' own first and last date: a series that
    # starts later than the others is not missing its earlier sessions.
    missing = con.sql("""
        WITH ranges AS (
            SELECT series_id, min(valid_date) AS lo, max(valid_date) AS hi
            FROM records GROUP BY series_id
        )
        SELECT r.series_id, s.day
        FROM ranges r JOIN sessions s ON s.day BETWEEN r.lo AND r.hi
        WHERE NOT EXISTS (
            SELECT 1 FROM records x WHERE x.series_id = r.series_id AND x.valid_date = s.day
        )
        ORDER BY s.day, r.series_id
    """).fetchall()
    return {
        "dates_not_sessions": _result(
            [{"series_id": s, "valid_date": d.isoformat()} for s, d in not_sessions],
            calendar=config.calendar,
        ),
        "sessions_missing": _result(
            [{"series_id": s, "valid_date": d.isoformat()} for s, d in missing],
            calendar=config.calendar,
        ),
    }


def _restatements(con, config: QualityConfig, ingest_rows: Mapping[str, int]) -> dict:
    # A record that is not its key's first is a restatement, made by the ingest
    # that created it. Timestamps are only ordered by, never fetched: fetching
    # TIMESTAMPTZ values would need pytz.
    by_ingest = con.sql("""
        WITH numbered AS (
            SELECT ingest_id, ingest_time,
                   row_number() OVER (
                       PARTITION BY series_id, valid_date ORDER BY knowledge_time
                   ) AS nth
            FROM records
        )
        SELECT ingest_id, count(*) AS restated
        FROM numbered WHERE nth > 1
        GROUP BY ingest_id
        ORDER BY min(ingest_time)
    """).fetchall()
    restated_keys = con.sql("""
        SELECT count(*) FROM (
            SELECT 1 FROM records GROUP BY series_id, valid_date HAVING count(*) > 1
        )
    """).fetchall()[0][0]

    rows = []
    for ingest_id, restated in by_ingest:
        size = ingest_rows.get(ingest_id)
        if not size:
            raise ValueError(f"no row count for ingest {ingest_id}")
        rows.append(
            {
                "ingest_id": ingest_id,
                "restated": restated,
                "rows_in_ingest": size,
                "fraction": round(restated / size, 6),
            }
        )
    over = [r for r in rows if r["fraction"] > config.restatement_warn_fraction]
    return {
        "status": "warn" if over else "ok",
        # Keys with more than one record, as in the build summary.
        "count": restated_keys,
        "examples": rows[:EXAMPLES],
        "ingests_over_threshold": [r["ingest_id"] for r in over],
    }


def _nulls(con) -> dict:
    rows = con.sql("""
        SELECT series_id, valid_date, ingest_id FROM records
        WHERE value IS NULL ORDER BY series_id, valid_date, ingest_id
    """).fetchall()
    by_series = con.sql("""
        SELECT series_id, count(*) FILTER (WHERE value IS NULL)
        FROM records GROUP BY series_id ORDER BY series_id
    """).fetchall()
    return _result(
        [{"series_id": s, "valid_date": d.isoformat(), "ingest_id": i} for s, d, i in rows],
        by_series=dict(by_series),
    )


def _outliers(con, config: QualityConfig) -> dict:
    if config.max_abs_value is None:
        return _skipped("no outlier bound configured for this source")
    rows = con.sql(
        """
        SELECT series_id, valid_date, value, ingest_id FROM records
        WHERE abs(value) > $bound
        ORDER BY abs(value) DESC, series_id, valid_date, ingest_id
        """,
        params={"bound": config.max_abs_value},
    ).fetchall()
    return _result(
        [
            {"series_id": s, "valid_date": d.isoformat(), "value": v, "ingest_id": i}
            for s, d, v, i in rows
        ],
        max_abs_value=config.max_abs_value,
    )


def _gaps(con) -> dict:
    # Information only: the calendar check already names every missing
    # session, and a gap rule would warn forever on real closures such as the
    # 1933 bank holiday.
    rows = con.sql("""
        WITH keys AS (SELECT DISTINCT series_id, valid_date FROM records),
        gaps AS (
            SELECT series_id,
                   lag(valid_date) OVER (PARTITION BY series_id ORDER BY valid_date) AS after,
                   valid_date AS before
            FROM keys
        )
        -- One entry per gap, listing the series that share it: a market
        -- closure is the same gap in every series.
        SELECT after, before, before - after AS days, list(series_id ORDER BY series_id)
        FROM gaps WHERE after IS NOT NULL
        GROUP BY after, before
        ORDER BY days DESC, before
        LIMIT 5
    """).fetchall()
    largest = [
        {"after": a.isoformat(), "before": b.isoformat(), "days": n, "series_ids": ids}
        for a, b, n, ids in rows
    ]
    return {
        "status": "info",
        "count": largest[0]["days"] if largest else 0,
        "examples": largest,
    }
