"""Quality checks: each one on hand-made records, with an injected calendar.

Records are made the real way, by running derive_records on raw ingests, so
the checks see exactly what a build gives them. The calendar is a fake: a
plain set of dates, so each test states the sessions it relies on.
"""

import json
from datetime import UTC, date, datetime, timedelta

import pytest
from strategies import make_ingest

from pitdata.quality import EXAMPLES, QualityConfig, check_quality, warnings
from pitdata.validated import LagRule, SourceConfig, derive_records

A, B = "S.A", "S.B"
# Jan 2024: Mon 1 is a holiday, then Tue 2 to Fri 5 and Mon 8 are sessions.
SESSIONS = frozenset(date(2024, 1, d) for d in (2, 3, 4, 5, 8))
T1 = datetime(2024, 6, 1, tzinfo=UTC)
T2 = T1 + timedelta(days=1)
VALIDATION = SourceConfig(LagRule("r", "1", 2))  # divide_by=1: values as written
CONFIG = QualityConfig(calendar="NYSE", max_abs_value=0.25, restatement_warn_fraction=0.01)


def fake_sessions(calendar, start, end):
    assert calendar == "NYSE"
    return frozenset(d for d in SESSIONS if start <= d <= end)


def day(n: int) -> date:
    return date(2024, 1, n)


def run(*ingests_rows, config=CONFIG, sessions=fake_sessions) -> dict:
    """Checks on the records derived from ingests at T1, T2, ... with these rows."""
    ingests = [make_ingest(T1 + timedelta(days=i), rows) for i, rows in enumerate(ingests_rows)]
    records = derive_records(ingests, VALIDATION)
    sizes = {meta["ingest_id"]: table.num_rows for meta, table in ingests}
    return check_quality(records, config, sizes, sessions=sessions)


def clean(series=(A,), days=(2, 3, 4, 5, 8), value=0.01) -> dict:
    return {(s, day(d)): value for s in series for d in days}


# --- a clean source ----------------------------------------------------------------


def test_clean_data_passes_every_check():
    checks = run(clean(series=(A, B)))
    assert {name: c["status"] for name, c in checks.items()} == {
        "dates_not_sessions": "ok",
        "sessions_missing": "ok",
        "restatements": "ok",
        "nulls": "ok",
        "outliers": "ok",
        "gaps": "info",
    }
    assert warnings(checks) == []


def test_no_records_passes_every_check():
    checks = run({})
    assert all(c["status"] in ("ok", "info") for c in checks.values())
    assert all(c["count"] == 0 for c in checks.values())


def test_the_report_is_json_and_deterministic():
    rows = {**clean(), (A, day(6)): None}
    assert json.dumps(run(rows)) == json.dumps(run(rows))


# --- calendar -------------------------------------------------------------------------


def test_a_date_that_is_not_a_session_warns():
    # Saturday 6 January 2024 is not in the fake calendar.
    checks = run({**clean(), (A, day(6)): 0.01})
    c = checks["dates_not_sessions"]
    assert (c["status"], c["count"]) == ("warn", 1)
    assert c["examples"] == [{"series_id": A, "valid_date": "2024-01-06"}]
    assert warnings(checks) == ["dates_not_sessions (1)"]


def test_a_missing_session_inside_the_range_warns():
    checks = run(clean(days=(2, 3, 5, 8)))
    c = checks["sessions_missing"]
    assert (c["status"], c["count"]) == ("warn", 1)
    assert c["examples"] == [{"series_id": A, "valid_date": "2024-01-04"}]


def test_missing_sessions_are_judged_per_series_within_its_own_range():
    # B starts later than A: B is not missing 2 and 3 January. Within its own
    # range, B is missing 5 January.
    rows = {**clean(series=(A,)), **clean(series=(B,), days=(4, 8))}
    c = run(rows)["sessions_missing"]
    assert c["examples"] == [{"series_id": B, "valid_date": "2024-01-05"}]


def test_sessions_before_the_first_or_after_the_last_date_are_not_missing():
    # Data from the 3rd to the 5th: the 2nd and the 8th are outside the range.
    assert run(clean(days=(3, 4, 5)))["sessions_missing"]["status"] == "ok"


def test_no_calendar_skips_both_calendar_checks_without_asking_for_one():
    def no_calls(*args):
        raise AssertionError("the calendar must not be consulted")

    config = QualityConfig(calendar=None, max_abs_value=0.25)
    checks = run(clean(), config=config, sessions=no_calls)
    assert checks["dates_not_sessions"]["status"] == "skipped"
    assert checks["sessions_missing"]["status"] == "skipped"


def test_the_calendar_is_asked_only_for_the_data_range():
    asked = []

    def recording(calendar, start, end):
        asked.append((start, end))
        return fake_sessions(calendar, start, end)

    run(clean(days=(3, 4)), sessions=recording)
    assert asked == [(day(3), day(4))]


# --- values: nulls and outliers cover every record ---------------------------------------


def test_nulls_are_counted_by_series_with_their_ingest():
    checks = run({**clean(series=(A, B)), (A, day(3)): None, (B, day(4)): None})
    c = checks["nulls"]
    assert (c["status"], c["count"], c["by_series"]) == ("warn", 2, {A: 1, B: 1})
    assert c["examples"][0] == {
        "series_id": A,
        "valid_date": "2024-01-03",
        "ingest_id": f"{T1:%Y%m%dT%H%M%SZ}_test",
    }


def test_a_null_later_corrected_is_still_reported():
    # Visible between T1 and T2, so a backtest could have read it.
    checks = run({**clean(), (A, day(3)): None}, clean())
    assert checks["nulls"]["count"] == 1


def test_outliers_are_above_the_bound_in_absolute_value():
    rows = {**clean(), (A, day(2)): 0.25, (A, day(3)): -0.26, (A, day(4)): 17.44}
    c = run(rows)["outliers"]
    assert (c["status"], c["count"]) == ("warn", 2)  # exactly 0.25 is not above
    # Largest first.
    assert [e["value"] for e in c["examples"]] == [17.44, -0.26]


def test_an_outlier_later_corrected_is_still_reported():
    checks = run({**clean(), (A, day(4)): 17.44}, clean())
    assert checks["outliers"]["count"] == 1


def test_no_bound_skips_the_outlier_check():
    config = QualityConfig(calendar="NYSE", max_abs_value=None)
    assert run({**clean(), (A, day(4)): 99.0}, config=config)["outliers"]["status"] == "skipped"


def test_examples_are_capped_but_the_count_is_not():
    many = {(A, date(2024, 1, 1) + timedelta(days=n)): None for n in range(EXAMPLES + 5)}
    c = run(many, config=QualityConfig())["nulls"]
    assert (c["count"], len(c["examples"])) == (EXAMPLES + 5, EXAMPLES)


# --- restatements ------------------------------------------------------------------------


def test_the_first_ingest_never_counts_as_a_restatement():
    c = run(clean())["restatements"]
    assert (c["status"], c["count"], c["examples"]) == ("ok", 0, [])


def test_an_ingest_that_restates_a_large_share_warns():
    # The second ingest has 5 rows and changes 1 of them: 20% > 1%.
    checks = run(clean(), {**clean(), (A, day(3)): 0.02})
    c = checks["restatements"]
    second = f"{T2:%Y%m%dT%H%M%SZ}_test"
    assert (c["status"], c["count"]) == ("warn", 1)
    assert c["examples"] == [
        {"ingest_id": second, "restated": 1, "rows_in_ingest": 5, "fraction": 0.2}
    ]
    assert c["ingests_over_threshold"] == [second]


def test_a_small_share_of_restatements_is_ok():
    config = QualityConfig(calendar="NYSE", max_abs_value=0.25, restatement_warn_fraction=0.5)
    c = run(clean(), {**clean(), (A, day(3)): 0.02}, config=config)["restatements"]
    assert (c["status"], c["count"]) == ("ok", 1)


def test_count_is_restated_keys_not_restating_records():
    # One key restated twice: two restating records, one restated key.
    checks = run(clean(), {**clean(), (A, day(3)): 0.02}, {**clean(), (A, day(3)): 0.03})
    c = checks["restatements"]
    assert c["count"] == 1
    assert [e["restated"] for e in c["examples"]] == [1, 1]


# --- gaps ------------------------------------------------------------------------------------


def test_gaps_report_the_largest_once_with_their_series_and_never_warn():
    rows = clean(series=(A, B), days=(2, 3, 8))  # 3 to 8 January: 5 days, in both
    c = run(rows, config=QualityConfig())["gaps"]
    assert (c["status"], c["count"]) == ("info", 5)
    assert c["examples"] == [
        {"after": "2024-01-03", "before": "2024-01-08", "days": 5, "series_ids": [A, B]},
        {"after": "2024-01-02", "before": "2024-01-03", "days": 1, "series_ids": [A, B]},
    ]


# --- configuration ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"calendar": "LSE"}, "unknown calendar"),
        ({"max_abs_value": 0}, "positive"),
        ({"restatement_warn_fraction": 1.5}, "between 0 and 1"),
    ],
)
def test_bad_configs_raise(kwargs, message):
    with pytest.raises(ValueError, match=message):
        QualityConfig(**kwargs)
