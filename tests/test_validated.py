import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pytest
from hypothesis import given
from hypothesis import strategies as st
from strategies import ingest_sequences, make_ingest

from pitdata.schema import RECORD_SCHEMA, SchemaError
from pitdata.sources.fama_french import VALIDATION as FF_VALIDATION
from pitdata.validated import LagRule, SourceConfig, derive_records

S = "FF3_DAILY.MKT_RF"
D = date(2024, 1, 15)
# Lag rule used in most tests: known by the end of the second month after.
# For any January 2024 date, the rule's instant is 2024-04-01T00:00Z.
CONFIG = SourceConfig(LagRule("test_rule", "1", months_after=2), divide_by=100.0)
RULE_JAN = datetime(2024, 4, 1, tzinfo=UTC)

# Ingest times. T1 is after the January rule instant, so January rows in the
# first ingest are "assumed" at RULE_JAN.
T1 = datetime(2024, 6, 1, 12, tzinfo=UTC)
T2, T3, T4, T5 = (T1 + timedelta(days=n) for n in (1, 2, 3, 4))


def records(ingests, config=CONFIG) -> list[dict]:
    return derive_records(ingests, config).to_pylist()


def summary(ingests, config=CONFIG) -> list[tuple]:
    """(value, knowledge_time, basis) per record, for compact assertions."""
    return [
        (r["value"], r["knowledge_time"], r["knowledge_basis"]) for r in records(ingests, config)
    ]


# --- reference implementation ------------------------------------------------


def reference_derive(ingests, config: SourceConfig) -> list[dict]:
    """The spec's rule, written as plainly as possible, independently of the SQL.

    Used by the differential property test: two implementations written in
    different ways rarely share a bug, so any disagreement points at one (D-025).
    """

    def rule_instant(d: date) -> datetime:
        month = d.month + config.lag_rule.months_after + 1
        year = d.year + (month - 1) // 12
        return datetime(year, (month - 1) % 12 + 1, 1, tzinfo=UTC)

    ordered = sorted(ingests, key=lambda mt: datetime.fromisoformat(mt[0]["ingest_time"]))
    first_id = ordered[0][0]["ingest_id"]
    last_value: dict = {}
    out = []
    for meta, table in ordered:
        time = datetime.fromisoformat(meta["ingest_time"])
        for row in table.to_pylist():
            key = (row["series_id"], row["valid_date"])
            # Same as the last value seen for this key (None == None counts): no record.
            if key in last_value and last_value[key] == row["value"]:
                continue
            last_value[key] = row["value"]
            if meta["ingest_id"] == first_id and rule_instant(key[1]) < time:
                known, basis = rule_instant(key[1]), "assumed"
            else:
                known, basis = time, "observed"
            value = None if row["value"] is None else row["value"] / config.divide_by
            out.append(
                {
                    "series_id": key[0],
                    "valid_date": key[1],
                    "value": value,
                    "knowledge_time": known,
                    "knowledge_basis": basis,
                    "ingest_time": time,
                    "ingest_id": meta["ingest_id"],
                    "source": meta["source"],
                }
            )
    return sorted(out, key=lambda r: (r["series_id"], r["valid_date"], r["knowledge_time"]))


# --- the spec's table: A, B, A, absent, A --------------------------------------


def test_spec_table_change_then_revert_then_absent():
    ingests = [
        make_ingest(T1, {(S, D): 1.0}),  # A: first appearance
        make_ingest(T2, {(S, D): 2.0}),  # B: changed
        make_ingest(T3, {(S, D): 1.0}),  # A: changed back, so a new record
        make_ingest(T4, {}),  # absent: nothing
        make_ingest(T5, {(S, D): 1.0}),  # A: same as ingest 3, the last one containing it
    ]
    assert summary(ingests) == [
        (0.01, RULE_JAN, "assumed"),
        (0.02, T2, "observed"),
        (0.01, T3, "observed"),
    ]


# --- nulls ---------------------------------------------------------------------


def test_null_to_value_and_value_to_null_each_create_a_record():
    ingests = [
        make_ingest(T1, {(S, D): None}),
        make_ingest(T2, {(S, D): 1.0}),
        make_ingest(T3, {(S, D): None}),
    ]
    assert summary(ingests) == [
        (None, RULE_JAN, "assumed"),
        (0.01, T2, "observed"),
        (None, T3, "observed"),
    ]


def test_null_to_null_creates_nothing():
    ingests = [make_ingest(T1, {(S, D): None}), make_ingest(T2, {(S, D): None})]
    assert summary(ingests) == [(None, RULE_JAN, "assumed")]


def test_a_first_value_that_is_null_still_gets_a_record():
    # Guards the `nth = 1` term in the SQL: without it, lag() is null on the
    # first row and NULL IS DISTINCT FROM NULL is false, so this row would vanish.
    assert summary([make_ingest(T1, {(S, D): None})]) == [(None, RULE_JAN, "assumed")]


# --- restatements and new keys -------------------------------------------------


def test_restatement_keeps_the_old_record():
    ingests = [make_ingest(T1, {(S, D): 1.0}), make_ingest(T2, {(S, D): 1.5})]
    assert summary(ingests) == [(0.01, RULE_JAN, "assumed"), (0.015, T2, "observed")]


def test_unchanged_value_creates_no_record():
    ingests = [make_ingest(T1, {(S, D): 1.0}), make_ingest(T2, {(S, D): 1.0})]
    assert len(records(ingests)) == 1


def test_key_first_seen_in_a_later_ingest_is_observed():
    # Even an old valid date the source newly added gets the later ingest's time.
    old = date(2020, 1, 2)
    ingests = [make_ingest(T1, {(S, D): 1.0}), make_ingest(T2, {(S, D): 1.0, (S, old): 3.0})]
    assert [
        (r["valid_date"], r["knowledge_basis"], r["knowledge_time"]) for r in records(ingests)
    ] == [
        (old, "observed", T2),
        (D, "assumed", RULE_JAN),
    ]


# --- lag rule and the cap ------------------------------------------------------


def test_rule_later_than_first_ingest_is_capped_and_observed():
    # Downloaded on 2024-03-10, before the rule's 2024-04-01: we really knew it then.
    early = datetime(2024, 3, 10, tzinfo=UTC)
    assert summary([make_ingest(early, {(S, D): 1.0})]) == [(0.01, early, "observed")]


def test_rule_equal_to_first_ingest_time_is_observed():
    # "Assumed" only when the rule is strictly earlier.
    assert summary([make_ingest(RULE_JAN, {(S, D): 1.0})]) == [(0.01, RULE_JAN, "observed")]


def test_first_ingest_is_found_from_metadata_even_when_empty():
    # The empty first ingest is still the first; ingest 2's rows are observed.
    ingests = [make_ingest(T1, {}), make_ingest(T2, {(S, D): 1.0})]
    assert summary(ingests) == [(0.01, T2, "observed")]


@pytest.mark.parametrize(
    ("valid", "months_after", "expected"),
    [
        (date(2026, 9, 15), 2, datetime(2026, 12, 1, tzinfo=UTC)),
        (date(2026, 9, 30), 2, datetime(2026, 12, 1, tzinfo=UTC)),
        (date(2026, 11, 1), 2, datetime(2027, 2, 1, tzinfo=UTC)),  # crosses a year
        (date(2026, 12, 31), 0, datetime(2027, 1, 1, tzinfo=UTC)),
    ],
)
def test_lag_rule_instant(valid, months_after, expected):
    assert LagRule("r", "1", months_after).instant(valid) == expected


def test_fama_french_config():
    assert FF_VALIDATION.lag_rule.name == "ff_month_end_plus_2"
    assert FF_VALIDATION.lag_rule.instant(date(2026, 9, 15)) == datetime(2026, 12, 1, tzinfo=UTC)
    assert FF_VALIDATION.divide_by == 100.0


def test_sql_rule_matches_python_rule_across_months():
    # Every month of two years, including year ends.
    days = [date(2023 + m // 12, m % 12 + 1, 28) for m in range(24)]
    ingest = make_ingest(datetime(2026, 1, 1, tzinfo=UTC), {(S, d): 1.0 for d in days})
    got = {r["valid_date"]: r["knowledge_time"] for r in records([ingest])}
    assert got == {d: CONFIG.lag_rule.instant(d) for d in days}


# --- units ---------------------------------------------------------------------


def test_percent_is_converted_to_decimal():
    assert records([make_ingest(T1, {(S, D): 0.25})])[0]["value"] == 0.0025


def test_change_detection_uses_values_before_conversion():
    # 13.9 and the next double up are different values as parsed, but both become
    # exactly 0.139 after dividing by 100. Comparing after conversion would lose
    # this restatement. (The Fama-French parser always turns the same text into
    # the same double, so this cannot arise from it today; another source can.)
    before, after = 13.9, 13.900000000000002
    assert before != after and before / 100 == after / 100
    ingests = [make_ingest(T1, {(S, D): before}), make_ingest(T2, {(S, D): after})]
    assert len(records(ingests)) == 2


# --- input checks and determinism --------------------------------------------------


def test_input_order_does_not_matter():
    ingests = [make_ingest(T1, {(S, D): 1.0}), make_ingest(T2, {(S, D): 2.0})]
    assert derive_records(ingests, CONFIG).equals(derive_records(ingests[::-1], CONFIG))


def test_two_runs_give_equal_tables():
    ingests = [make_ingest(T1, {(S, D): 1.0}), make_ingest(T2, {(S, D): 2.0})]
    assert derive_records(ingests, CONFIG).equals(derive_records(ingests, CONFIG))


def test_output_matches_record_schema():
    result = derive_records([make_ingest(T1, {(S, D): 1.0})], CONFIG)
    assert result.schema.equals(RECORD_SCHEMA, check_metadata=True)


def test_no_ingests_gives_an_empty_table():
    assert derive_records([], CONFIG).schema.equals(RECORD_SCHEMA)


def test_duplicate_ingest_time_raises():
    with pytest.raises(ValueError, match="same ingest_time"):
        derive_records([make_ingest(T1, {(S, D): 1.0}), make_ingest(T1, {(S, D): 2.0})], CONFIG)


def test_mixed_sources_raise():
    ingests = [make_ingest(T1, {(S, D): 1.0}), make_ingest(T2, {(S, D): 1.0}, source="fred")]
    with pytest.raises(ValueError, match="one source"):
        derive_records(ingests, CONFIG)


def test_rows_not_matching_meta_raise():
    meta, table = make_ingest(T1, {(S, D): 1.0})
    with pytest.raises(ValueError, match="do not match"):
        derive_records([({**meta, "ingest_id": "other"}, table)], CONFIG)


def test_duplicate_key_within_one_ingest_raises():
    meta, table = make_ingest(T1, {(S, D): 1.0})
    with pytest.raises(ValueError, match="more than once"):
        derive_records([(meta, pa.concat_tables([table, table]))], CONFIG)


def test_valid_date_after_ingest_time_is_rejected_by_the_schema():
    # A value cannot be known before the period it describes; the record schema's
    # existing check is the backstop.
    future = (T1 + timedelta(days=30)).date()
    with pytest.raises(SchemaError, match="knowledge_time before valid_date"):
        derive_records([make_ingest(T1, {(S, future): 1.0})], CONFIG)


# --- time zone -------------------------------------------------------------------

_TOKYO_SCRIPT = """
import json, sys
from datetime import UTC, date, datetime
import pyarrow as pa
from pitdata.validated import LagRule, SourceConfig, derive_records
t = datetime(2026, 6, 1, tzinfo=UTC)
meta = {"ingest_id": "i1", "ingest_time": t.isoformat(), "source": "s"}
table = pa.table({
    "series_id": ["x"], "valid_date": pa.array([date(2026, 1, 15)]), "value": [1.0],
    "ingest_time": pa.array([t], type=pa.timestamp("us", tz="UTC")),
    "ingest_id": ["i1"], "source": ["s"],
})
out = derive_records([(meta, table)], SourceConfig(LagRule("r", "1", 2)))
print(json.dumps(out.column("knowledge_time")[0].as_py().isoformat()))
"""


def test_result_does_not_depend_on_the_machine_time_zone():
    # A separate process, because DuckDB picks up the local zone when it starts.
    # Without SET TimeZone = 'UTC', this gives 2026-03-31T15:00Z: nine hours early.
    env = {**os.environ, "TZ": "Asia/Tokyo"}
    out = subprocess.run(
        [sys.executable, "-c", _TOKYO_SCRIPT], env=env, capture_output=True, text=True, check=True
    )
    assert json.loads(out.stdout) == "2026-04-01T00:00:00+00:00"


# --- differential property test ------------------------------------------------------


@given(ingest_sequences(), st.integers(0, 3))
def test_sql_matches_reference_implementation(ingests, months_after):
    config = SourceConfig(LagRule("r", "1", months_after), divide_by=100.0)
    assert derive_records(ingests, config).to_pylist() == reference_derive(ingests, config)
