"""The as-of query: get_from on hand-built records, get on files written by pit build."""

import io
import json
import os
import subprocess
import sys
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pitdata.build import build, read_validated
from pitdata.query import RESULT_SCHEMA, QueryError, get, get_from
from pitdata.schema import RECORD_SCHEMA
from pitdata.sources import CONFIGS
from pitdata.sources.fama_french import ingest_ff3_daily

S, S2 = "FF3_DAILY.MKT_RF", "FF3_DAILY.SMB"
D = date(2024, 1, 2)
K1 = datetime(2024, 3, 1, tzinfo=UTC)  # first known
K2 = datetime(2024, 6, 1, tzinfo=UTC)  # restated
FIXTURE = Path(__file__).parent / "fixtures" / "ff3_daily_sample.csv"


def make_records(rows: list[tuple]) -> pa.Table:
    """rows: (series_id, valid_date, value, knowledge_time, knowledge_basis)."""
    return pa.table(
        {
            "series_id": pa.array([r[0] for r in rows], type=pa.string()),
            "valid_date": pa.array([r[1] for r in rows], type=pa.date32()),
            "value": pa.array([r[2] for r in rows], type=pa.float64()),
            "knowledge_time": pa.array([r[3] for r in rows], type=pa.timestamp("us", tz="UTC")),
            "knowledge_basis": pa.array([r[4] for r in rows], type=pa.string()),
            "ingest_time": pa.array([r[3] for r in rows], type=pa.timestamp("us", tz="UTC")),
            "ingest_id": pa.array([f"ingest-{r[3]:%Y%m%d}" for r in rows], type=pa.string()),
            "source": pa.array(["ken_french"] * len(rows), type=pa.string()),
        }
    )


RESTATED = make_records([(S, D, 0.01, K1, "assumed"), (S, D, 0.02, K2, "observed")])


def values(table: pa.Table) -> list:
    return table.column("value").to_pylist()


# --- the as-of rule ---------------------------------------------------------------


def test_before_the_first_record_nothing_is_known():
    assert get_from(RESTATED, S, D, D, K1 - timedelta(microseconds=1)).num_rows == 0


def test_between_records_the_old_value_is_returned():
    # Filter first, then pick the latest. Picking first would choose the
    # restatement, filter it out, and return nothing for this key.
    assert values(get_from(RESTATED, S, D, D, K2 - timedelta(microseconds=1))) == [0.01]


def test_at_exactly_the_restatement_time_the_new_value_is_returned():
    # The rule is knowledge_time <= asof.
    assert values(get_from(RESTATED, S, D, D, K2)) == [0.02]


def test_after_both_records_the_new_value_is_returned():
    result = get_from(RESTATED, S, D, D, K2 + timedelta(days=365))
    assert result.to_pylist() == [
        {
            "series_id": S,
            "valid_date": D,
            "value": 0.02,
            "knowledge_time": K2,
            "knowledge_basis": "observed",
            "ingest_id": "ingest-20240601",
        }
    ]


def test_a_null_latest_value_is_returned_as_null_not_the_older_value():
    # We knew the value was missing. An aggregate that skips nulls would return 0.01.
    records = make_records([(S, D, 0.01, K1, "assumed"), (S, D, None, K2, "observed")])
    assert values(get_from(records, S, D, D, K2)) == [None]
    assert values(get_from(records, S, D, D, K1)) == [0.01]


# --- range, series and result shape ---------------------------------------------


DAYS = [date(2024, 1, d) for d in (2, 3, 4, 5)]
GRID = make_records([(s, d, float(i), K1, "assumed") for s in (S, S2) for i, d in enumerate(DAYS)])


def test_date_range_is_inclusive_at_both_ends():
    result = get_from(GRID, S, DAYS[1], DAYS[2], K1)
    assert result.column("valid_date").to_pylist() == [DAYS[1], DAYS[2]]


def test_single_day_range():
    assert get_from(GRID, S, DAYS[0], DAYS[0], K1).num_rows == 1


def test_multiple_series_sorted_by_series_then_date():
    result = get_from(GRID, [S2, S], DAYS[0], DAYS[1], K1)
    assert [(r["series_id"], r["valid_date"]) for r in result.to_pylist()] == [
        (S, DAYS[0]),
        (S, DAYS[1]),
        (S2, DAYS[0]),
        (S2, DAYS[1]),
    ]


def test_unknown_series_gives_an_empty_table_with_the_result_schema():
    result = get_from(GRID, "NOPE", DAYS[0], DAYS[-1], K1)
    assert result.num_rows == 0
    assert result.schema.equals(RESULT_SCHEMA)


def test_result_schema_is_utc_and_exact():
    result = get_from(GRID, S, DAYS[0], DAYS[-1], K1)
    assert result.schema.equals(RESULT_SCHEMA)
    assert result.schema.field("knowledge_time").type == pa.timestamp("us", tz="UTC")


def test_ties_on_key_and_knowledge_time_raise():
    records = make_records([(S, D, 0.01, K1, "assumed"), (S, D, 0.02, K1, "assumed")])
    with pytest.raises(QueryError, match="same key and knowledge_time"):
        get_from(records, S, D, D, K2)


# --- input checks ----------------------------------------------------------------


@pytest.mark.parametrize(
    "asof",
    [
        datetime(2024, 6, 1),  # noqa: DTZ001 - naive on purpose
        datetime(2024, 6, 1, tzinfo=ZoneInfo("America/New_York")),
        datetime(2024, 1, 13, tzinfo=ZoneInfo("Europe/London")),  # offset 0, not UTC
    ],
)
def test_asof_must_be_utc(asof):
    with pytest.raises(ValueError, match="asof must be time-zone-aware UTC"):
        get_from(RESTATED, S, D, D, asof)


def test_asof_as_a_plain_date_is_refused():
    # "As of 2024-06-01": the start or the end of that day? Refused, not guessed.
    with pytest.raises(TypeError, match="asof must be a datetime"):
        get_from(RESTATED, S, D, D, date(2024, 6, 1))


@pytest.mark.parametrize("field", ["start", "end"])
def test_start_and_end_must_be_dates_not_datetimes(field):
    # datetime is a subclass of date; a bare isinstance(x, date) check would pass it.
    args = {"start": D, "end": D, field: datetime(2024, 1, 2, tzinfo=UTC)}
    with pytest.raises(TypeError, match=f"{field} must be a date"):
        get_from(RESTATED, S, args["start"], args["end"], K2)


def test_end_before_start_raises():
    with pytest.raises(ValueError, match="before start"):
        get_from(RESTATED, S, D, D - timedelta(days=1), K2)


@pytest.mark.parametrize("series", [[], [S, 3]])
def test_series_must_be_strings(series):
    with pytest.raises(ValueError, match="series must be"):
        get_from(RESTATED, series, D, D, K2)


def test_records_are_validated():
    with pytest.raises(ValueError, match="missing"):
        get_from(RESTATED.drop_columns(["knowledge_basis"]), S, D, D, K2)


# --- get: the same query over validated files -----------------------------------


def zipped(csv: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("F-F_Research_Data_Factors_daily.csv", csv)
    return buf.getvalue()


@pytest.fixture
def validated(tmp_path) -> Path:
    """Sample file -> ingest -> build, then a restatement -> ingest -> build."""
    raw, out = tmp_path / "raw", tmp_path / "validated"
    t1 = datetime(2026, 11, 20, 21, tzinfo=UTC)
    ingest_ff3_daily(raw, t1, fetch=lambda url: zipped(FIXTURE.read_bytes()))
    restated = FIXTURE.read_bytes().replace(b"-1.07", b"-1.17")
    ingest_ff3_daily(raw, t1 + timedelta(days=1), fetch=lambda url: zipped(restated))
    build(raw, out, CONFIGS)
    return out


ALL = ["FF3_DAILY.MKT_RF", "FF3_DAILY.SMB", "FF3_DAILY.HML", "FF3_DAILY.RF"]


def test_get_returns_decimals_end_to_end(validated):
    # The spec's acceptance check: 0.25 percent in the file is 0.0025 here.
    asof = datetime(2026, 12, 31, tzinfo=UTC)
    result = get("FF3_DAILY.MKT_RF", date(2024, 1, 2), date(2024, 1, 31), asof, root=validated)
    # -1.07 restated to -1.17. Expected values are written as parsed / 100: -1.17 has
    # no exact binary form, so -1.17 / 100 is -0.011699999999999999, not -0.0117.
    assert values(result)[:2] == [0.25 / 100, -1.17 / 100]
    assert result.num_rows == 7


def test_get_sees_the_restatement_only_after_it_was_made(validated):
    day = date(2024, 1, 3)
    before = datetime(2026, 11, 21, 20, tzinfo=UTC)  # after ingest 1, before ingest 2
    after = datetime(2026, 11, 21, 21, tzinfo=UTC)  # exactly ingest 2's time
    assert values(get("FF3_DAILY.MKT_RF", day, day, before, root=validated)) == [-1.07 / 100]
    assert values(get("FF3_DAILY.MKT_RF", day, day, after, root=validated)) == [-1.17 / 100]


@pytest.mark.parametrize(
    "asof",
    [
        datetime(2024, 1, 31, tzinfo=UTC),  # before any assumed time: nothing
        datetime(2024, 4, 1, tzinfo=UTC),  # January's assumed time
        datetime(2026, 11, 21, 20, tzinfo=UTC),
        datetime(2027, 1, 1, tzinfo=UTC),
    ],
)
def test_get_and_get_from_agree(validated, asof):
    records = read_validated(validated / "ken_french.parquet")
    start, end = date(2024, 1, 1), date(2024, 12, 31)
    assert get(ALL, start, end, asof, root=validated).equals(
        get_from(records, ALL, start, end, asof)
    )


def test_get_ignores_temp_files(validated):
    (validated / ".tmp-ken_french-dead.parquet").write_bytes(b"half a file")
    asof = datetime(2027, 1, 1, tzinfo=UTC)
    assert get(ALL, date(2024, 1, 1), date(2024, 12, 31), asof, root=validated).num_rows == 28


def test_get_rejects_a_file_with_the_wrong_schema(validated):
    pq.write_table(pa.table({"x": [1]}), validated / "other.parquet")
    with pytest.raises(QueryError, match="other.parquet: not a validated records file"):
        get(S, D, D, K2, root=validated)


@pytest.mark.parametrize("make", ["missing", "empty"])
def test_get_without_validated_files_says_to_run_build(tmp_path, make):
    root = tmp_path / "validated"
    if make == "empty":
        root.mkdir()
    with pytest.raises(QueryError, match="run `pit build` first"):
        get(S, D, D, K2, root=root)


def test_record_schema_files_are_accepted_by_get(tmp_path):
    root = tmp_path / "validated"
    root.mkdir()
    pq.write_table(RECORD_SCHEMA.empty_table(), root / "x.parquet")
    assert get(S, D, D, K2, root=root).num_rows == 0


# --- machine time zone -----------------------------------------------------------

_TOKYO_SCRIPT = """
import json
from datetime import UTC, date, datetime
import pyarrow as pa
from pitdata.query import get_from
t = lambda *a: datetime(*a, tzinfo=UTC)
records = pa.table({
    "series_id": ["x", "x"], "valid_date": pa.array([date(2024, 1, 2)] * 2),
    "value": [1.0, 2.0],
    "knowledge_time": pa.array([t(2024, 3, 1), t(2024, 6, 1)], type=pa.timestamp("us", tz="UTC")),
    "knowledge_basis": ["assumed", "observed"],
    "ingest_time": pa.array([t(2024, 3, 1), t(2024, 6, 1)], type=pa.timestamp("us", tz="UTC")),
    "ingest_id": ["a", "b"], "source": ["s", "s"],
})
out = get_from(records, "x", date(2024, 1, 2), date(2024, 1, 2), t(2024, 6, 1))
row = out.to_pylist()[0]
print(json.dumps([row["value"], row["knowledge_time"].isoformat(), str(out.schema.field("knowledge_time").type)]))
"""


def test_results_do_not_depend_on_the_machine_time_zone():
    env = {**os.environ, "TZ": "Asia/Tokyo"}
    out = subprocess.run(
        [sys.executable, "-c", _TOKYO_SCRIPT], env=env, capture_output=True, text=True, check=True
    )
    assert json.loads(out.stdout) == [2.0, "2024-06-01T00:00:00+00:00", "timestamp[us, tz=UTC]"]
