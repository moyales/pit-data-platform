"""Property tests: the guarantees pitdata promises, on random ingest histories.

Spec section 5. Each test generates 1 to 6 raw ingests, derives records with
`derive_records`, queries them with `get_from`, and checks one promise at
several query times T. The differential test in test_validated.py checks that
the SQL follows the rule; these check that the rule, plus the query, keeps the
promises. A misunderstanding shared by the SQL and the reference
implementation would pass that test and fail these.

How T is chosen (D-032): a mix of random times and boundaries (every ingest
time and lag-rule instant, and one microsecond either side), because
off-by-one mistakes such as `<` for `<=` only show at boundaries.

`hypothesis.event()` labels the interesting cases, so
`pytest --hypothesis-show-statistics` shows whether they were generated.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pyarrow as pa
from hypothesis import event, given
from hypothesis import strategies as st
from strategies import DATES, SERIES, ingest_sequences, ingest_time

from pitdata.query import RESULT_SCHEMA, get_from
from pitdata.validated import LagRule, SourceConfig, derive_records

DIVIDE_BY = 100.0
ONE_US = timedelta(microseconds=1)
# Before every rule instant and ingest, and after all of them.
BEFORE_ALL = datetime(2024, 1, 1, tzinfo=UTC)
AFTER_ALL = datetime(2025, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class History:
    ingests: list[tuple[dict, pa.Table]]  # in no particular order
    config: SourceConfig
    times: list[datetime]  # query times T to check

    @property
    def first_ingest(self) -> datetime:
        return min(ingest_time(meta) for meta, _ in self.ingests)

    def known_by(self, t: datetime) -> list[tuple[dict, pa.Table]]:
        """P: the ingests made at or before t."""
        return [(m, table) for m, table in self.ingests if ingest_time(m) <= t]

    def prefixes(self) -> list[list[tuple[dict, pa.Table]]]:
        """The first 1, 2, ..., n ingests in time order."""
        ordered = sorted(self.ingests, key=lambda mt: ingest_time(mt[0]))
        return [ordered[: n + 1] for n in range(len(ordered))]


@st.composite
def histories(draw) -> History:
    ingests = draw(ingest_sequences())
    config = SourceConfig(LagRule("r", "1", draw(st.integers(0, 3))), divide_by=DIVIDE_BY)
    instants = [ingest_time(m) for m, _ in ingests] + [config.lag_rule.instant(d) for d in DATES]
    boundaries = sorted({t + d for t in instants for d in (-ONE_US, timedelta(0), ONE_US)})
    boundaries += [BEFORE_ALL, AFTER_ALL]
    random_time = st.datetimes(
        min_value=BEFORE_ALL.replace(tzinfo=None),
        max_value=AFTER_ALL.replace(tzinfo=None),
        timezones=st.just(UTC),
    )
    times = draw(
        st.lists(st.one_of(st.sampled_from(boundaries), random_time), min_size=1, max_size=4)
    )
    return History(ingests, config, times)


def query(records: pa.Table, t: datetime) -> pa.Table:
    """Everything known at t, for every series and date the histories use."""
    return get_from(records, list(SERIES), min(DATES), max(DATES), t)


def rows(table: pa.Table) -> list[tuple]:
    """Rows as tuples: hashable for set comparisons, and readable in a failure."""
    return [tuple(r.values()) for r in table.to_pylist()]


def label_cases(h: History, full: pa.Table, t: datetime) -> None:
    """Record which interesting situations this (history, T) exercises."""
    if t < h.first_ingest:
        event("T before the first ingest")
    elif any(ingest_time(m) > t for m, _ in h.ingests):
        event("T between ingests")
    else:
        event("T after every ingest")
    times = list(zip(_keys(full), full.column("knowledge_time").to_pylist()))
    known = {k for k, kt in times if kt <= t}
    if any(kt > t and k in known for k, kt in times):
        event("a key known at T is restated after T")


def label_history(full: pa.Table) -> None:
    """Record which interesting value sequences the history contains."""
    values: dict[tuple, list] = {}
    for key, value in zip(_keys(full), full.column("value").to_pylist()):
        values.setdefault(key, []).append(value)  # full is sorted by knowledge_time
    if any(len(v) > 1 for v in values.values()):
        event("a key has more than one record")
    if any(
        v[i] == v[j] for v in values.values() for i in range(len(v)) for j in range(i + 2, len(v))
    ):
        event("a value changes and then reverts (A->B->A)")
    if any(v[-1] is None for v in values.values()):
        event("a key's latest value is null")


def _keys(table: pa.Table) -> list[tuple]:
    return list(zip(table.column("series_id").to_pylist(), table.column("valid_date").to_pylist()))


def reference_as_of(records: pa.Table, t: datetime) -> list[tuple]:
    """The as-of rule written as plainly as possible, independently of the SQL:
    for each key, the record with the greatest knowledge_time <= t."""
    latest: dict[tuple, dict] = {}
    for r in records.to_pylist():
        key = (r["series_id"], r["valid_date"])
        if r["knowledge_time"] <= t and (
            key not in latest or r["knowledge_time"] > latest[key]["knowledge_time"]
        ):
            latest[key] = r
    return [tuple(latest[k][c] for c in RESULT_SCHEMA.names) for k in sorted(latest)]


# --- 0. the query is the as-of rule ----------------------------------------------


@given(histories())
def test_query_matches_the_as_of_rule_written_in_python(h):
    """Properties 1 to 5 compare the query with itself, so a mistake that hits
    every call alike (such as `<` for `<=`) passes them all. This one states
    the rule directly, and the boundary query times are where it bites."""
    full = derive_records(h.ingests, h.config)
    for t in h.times:
        label_cases(h, full, t)
        if t in set(full.column("knowledge_time").to_pylist()):
            event("T equals a record's knowledge_time")
        assert rows(query(full, t)) == reference_as_of(full, t)


# --- 1. append-invariance --------------------------------------------------------


@given(histories())
def test_append_invariance(h):
    """Ingests made after T never change the answer at T (for T >= first ingest).

    The key property. With P the ingests made by T, deriving from P alone and
    deriving from every ingest must give the same answer at T: whole rows,
    including knowledge_time, basis and ingest_id, not just values.
    """
    full = derive_records(h.ingests, h.config)
    for t in h.times:
        if t < h.first_ingest:
            continue  # P is empty here; property 2 covers this case
        label_cases(h, full, t)
        assert rows(query(derive_records(h.known_by(t), h.config), t)) == rows(query(full, t))


# --- 2. before the first ingest, only assumptions --------------------------------


@given(histories())
def test_before_the_first_ingest_everything_visible_is_assumed(h):
    """Before any download, the only knowledge is placed by the lag rule.

    Observed records have knowledge_time equal to an ingest time, so none can
    be visible before the first ingest. Anything visible must say "assumed".
    """
    full = derive_records(h.ingests, h.config)
    for t in h.times:
        if t >= h.first_ingest:
            continue
        result = query(full, t)
        if result.num_rows:
            event("rows visible before the first ingest")
        assert set(result.column("knowledge_basis").to_pylist()) <= {"assumed"}


# --- 3. no leaks -----------------------------------------------------------------


@given(histories())
def test_no_row_is_known_after_the_query_time(h):
    full = derive_records(h.ingests, h.config)
    for t in h.times:
        label_cases(h, full, t)
        assert all(kt <= t for kt in query(full, t).column("knowledge_time").to_pylist())


# --- 4. causality ----------------------------------------------------------------


@given(histories())
def test_later_ingests_never_change_or_remove_earlier_records(h):
    """derive(first k ingests) is a subset of derive(all ingests), for every k.

    This is why property 1 holds: records are only ever added, and each one
    depends only on its own ingest and the ones before it.
    """
    full = set(rows(derive_records(h.ingests, h.config)))
    for prefix in h.prefixes():
        assert set(rows(derive_records(prefix, h.config))) <= full


# --- 5. final state --------------------------------------------------------------


@given(histories())
def test_after_every_ingest_each_key_has_its_last_seen_value(h):
    """As of a time after every ingest, each key shows the value in the last
    ingest that contained it, converted. Absent from later ingests means
    unchanged, not deleted; null is a value and is returned as null.

    The expected answer is computed here in plain Python, independently of
    derive_records and the query.
    """
    expected = {}
    for meta, table in sorted(h.ingests, key=lambda mt: ingest_time(mt[0])):
        for r in table.to_pylist():
            value = None if r["value"] is None else r["value"] / DIVIDE_BY
            expected[(r["series_id"], r["valid_date"])] = value

    full = derive_records(h.ingests, h.config)
    label_history(full)
    result = query(full, AFTER_ALL)
    assert dict(zip(_keys(result), result.column("value").to_pylist())) == expected
