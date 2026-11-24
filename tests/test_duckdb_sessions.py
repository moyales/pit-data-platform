"""The shared DuckDB database is safe only if sessions stay separate (D-031).

These tests pin down the DuckDB behaviour that design relies on: a setting or
a registered table in one session is invisible to every other session. If a
DuckDB upgrade changes that, these fail before any wrong answer can.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime

import duckdb
import pyarrow as pa
import pytest

from pitdata import _duckdb
from pitdata._duckdb import session
from pitdata.query import get_from
from pitdata.schema import RECORD_SCHEMA


def setting(con, name: str) -> str:
    return con.sql(f"SELECT current_setting('{name}')").fetchall()[0][0]


def test_a_session_is_utc():
    with session() as con:
        assert setting(con, "TimeZone") == "UTC"


def test_a_setting_does_not_reach_the_next_session():
    with session() as con:
        con.execute("SET TimeZone = 'Asia/Tokyo'")
    with session() as con:
        assert setting(con, "TimeZone") == "UTC"


def test_a_setting_does_not_reach_a_concurrent_session():
    with session() as a, session() as b:
        a.execute("SET TimeZone = 'Asia/Tokyo'")
        assert setting(b, "TimeZone") == "UTC"


def test_a_registered_table_is_invisible_to_other_sessions():
    with session() as a, session() as b:
        a.register("records", pa.table({"x": [1]}))
        with pytest.raises(duckdb.CatalogException):
            b.sql("SELECT * FROM records").fetchall()
        b.register("records", pa.table({"x": [2]}))
        assert a.sql("SELECT x FROM records").fetchall() == [(1,)]
    with session() as c, pytest.raises(duckdb.CatalogException):
        c.sql("SELECT * FROM records").fetchall()


def test_sessions_share_one_database():
    assert _duckdb._shared_database() is _duckdb._shared_database()


def test_a_forked_process_opens_its_own_database(monkeypatch):
    before = _duckdb._shared_database()
    # Pretend the database was opened by a parent process.
    monkeypatch.setattr(_duckdb, "_owner_pid", -1)
    assert _duckdb._shared_database() is not before


def test_concurrent_queries_each_see_only_their_own_records():
    # Eight threads, each with its own `records` table holding one value.
    # A leak between sessions would return another thread's value.
    t = datetime(2024, 6, 1, tzinfo=UTC)

    def one_record(value: float) -> pa.Table:
        return pa.table(
            {
                "series_id": ["S"],
                "valid_date": pa.array([date(2024, 1, 2)]),
                "value": [value],
                "ingest_time": pa.array([t], type=pa.timestamp("us", tz="UTC")),
                "ingest_id": ["i"],
                "source": ["src"],
                "knowledge_time": pa.array([t], type=pa.timestamp("us", tz="UTC")),
                "knowledge_basis": ["observed"],
            },
            schema=RECORD_SCHEMA,
        )

    def ask(value: float) -> list:
        results = []
        for _ in range(20):
            out = get_from(one_record(value), "S", date(2024, 1, 2), date(2024, 1, 2), t)
            results.extend(out.column("value").to_pylist())
        return results

    values = [float(n) for n in range(8)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        for value, results in zip(values, pool.map(ask, values), strict=True):
            assert results == [value] * 20
