"""DuckDB sessions for pitdata: one shared in-memory database, one session per call.

Opening a new DuckDB database costs about 25 ms; opening a cursor on an
existing one costs about 0.01 ms. A backtest asks as-of questions once per
rebalance date, so per-call databases would add minutes (D-031).

A cursor is a separate connection to the same database, with its own session:
its settings (such as TimeZone) and the tables it registers are invisible to
every other cursor. test_duckdb_sessions.py checks both, so a DuckDB upgrade
that changed this would fail loudly. Nothing is ever written to the shared
database itself; it holds no state between calls.
"""

import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager

import duckdb

_lock = threading.Lock()
_database: duckdb.DuckDBPyConnection | None = None
_owner_pid: int | None = None


def _shared_database() -> duckdb.DuckDBPyConnection:
    global _database, _owner_pid
    with _lock:
        # A DuckDB database must not be used across fork(): a child process
        # (for example a multiprocessing worker) opens its own.
        if _database is None or _owner_pid != os.getpid():
            _database = duckdb.connect()
            _owner_pid = os.getpid()
        return _database


@contextmanager
def session() -> Iterator[duckdb.DuckDBPyConnection]:
    """A private DuckDB session with TimeZone set to UTC, closed on exit."""
    con = _shared_database().cursor()
    try:
        # Without this, casting a DATE to TIMESTAMPTZ uses the machine's local
        # zone: in Tokyo, a lag-rule instant nine hours early, which is
        # lookahead (D-024).
        con.execute("SET TimeZone = 'UTC'")
        yield con
    finally:
        con.close()
