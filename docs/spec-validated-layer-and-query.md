# Spec: validated layer, as-of query and property tests (Week 2)

> **Status (2026-11-23):** tasks 1 to 6 are implemented and merged. Still open: task 7 (ETF price research) and the remaining docs in task 8.

*Agreed Nov 19, 2026. Implements decisions D-020 to D-024 in `docs/design-notes.md`. Builds on `docs/spec-raw-layer-and-ff-ingest.md` and replaces its "Not in this step (Week 2)" section, which had a bug (see D-020).*

## Scope

**In.** Fama-French end to end, from raw ingests to a tested as-of query:

- the validated layer: knowledge-time derivation, unit conversion, a deterministic rebuild;
- `get(series, start, end, asof)` with DuckDB;
- property tests for append-invariance;
- a quality report, including checks against a historical exchange calendar;
- an investigation of free sources for ETF prices (findings only, no code).

**Out (moved to Week 3).** The ETF price ingest, next to the ALFRED macro ingest. The project's central claim, the as-of query with the append-invariance test, can be shown completely on one source. The price ingest depends on facts not yet confirmed: which free source gives unadjusted prices plus split and dividend data, and on what terms.

## Task order

1. **This spec** and design-log entries D-020 to D-024.
2. **`validated.py`:** `derive_records`, a pure function, and its tests.
3. **`pit build`:** rebuild `data/validated/` from raw.
4. **`query.py`:** `get(...)` and its tests.
5. **Property tests:** append-invariance, no leaks, causality, final state.
6. **Quality report**, after a short calendar investigation.
7. **ETF price investigation:** a findings document only.
8. **Docs:** design log and README (status, roadmap, limitations).

One pull request per task, or per pair of small tasks. Explain the approach before writing each component.

## 2. Knowledge-time derivation (`validated.py`)

```python
def derive_records(ingests: Sequence[tuple[dict, pa.Table]], config: SourceConfig) -> pa.Table
```

*Amended by D-026: the spec originally passed only a `LagRule`. `SourceConfig` holds the lag rule and the unit conversion, and each source declares its own.*

- **Input:** every finished raw ingest of one source, as `(meta, table)` pairs, where the table is `read_ingest(path)`.
- **Ordering:** the function sorts the input by `ingest_time` itself, and raises on two ingests with the same `ingest_time`.
- **Output:** a table that passes `validate(..., RECORD_SCHEMA)`, sorted by `series_id`, `valid_date`, `knowledge_time`.
- **Purity:** no files, no clock. The same input always gives an identical table.

### When a record is created (D-020)

For each `(series_id, valid_date)`, walk the ingests that contain it, in `ingest_time` order. Create a record when either:

- the key appears for the first time; or
- its value differs from the value in the **previous ingest that contained the key**.

"Differs" uses `IS DISTINCT FROM` semantics: null to value, value to null, and value to a different value all count; null to null does not. Values are compared **as parsed**, in the source's units, before any conversion. Fama-French values are parsed from two-decimal text, so equality is exact.

| Ingest | 1 | 2 | 3 | 4 (key absent) | 5 |
|---|---|---|---|---|---|
| Value | A | B | A | – | A |
| Record? | yes, A | yes, B | yes, A | no | no (same as ingest 3) |

The old rule ("each distinct value gets one record") produces only A@1 and B@2, so a query as of ingest 3 would wrongly return B.

### Which knowledge time a record gets (D-021)

- **Records from the first ingest of a source:** `knowledge_time = min(lag_rule(valid_date), ingest_time)`.
  - `knowledge_basis = "assumed"` when the lag rule's instant is strictly earlier than `ingest_time`.
  - `"observed"` otherwise: we downloaded the file before the rule's date, so the download time is the better evidence.
- **Records from any later ingest:** `knowledge_time = ingest_time`, `knowledge_basis = "observed"`. This includes rows that first appear in a later ingest, even old valid dates the source has newly added.
- **Fama-French lag rule:** 00:00 UTC on the first day of the **third** month after the month of `valid_date`. In words: assume the value became known by the end of the second month after the month it describes.
  - Example: `valid_date` 2026-09-15 gives 2026-12-01T00:00Z.
  - Evidence: the 2026-10-26 file covered data through 2026-09-30, a lag of about 4 weeks after month end. The rule allows about 9 weeks. Too late is safe; too early is lookahead.
- **The rule is a named, versioned value** (`LagRule(name="ff_month_end_plus_2", version="1")`), so a later change is visible.

### Units and identity

- **Units:** Fama-French values are divided by 100 (percent to decimal) **after** change detection.
- **Ingest columns:** `ingest_time` and `ingest_id` on each record are those of the ingest that created it.
- **`source`:** passed through unchanged.

### Withdrawn rows

A key absent from a later ingest produces no record; the last value stays visible (see the table above). Withdrawals are a known limitation, carried over from Week 1.

### Tests

- The table above, row by row.
- Null to value and value to null each create a record. Null to null does not.
- A restatement in ingest 2 gives two records, and the older one is kept.
- First-ingest records get the lag rule. The `min(rule, ingest_time)` case gives `observed`. A rule instant exactly equal to `ingest_time` gives `observed`.
- A key that first appears in ingest 2 gets `observed` at ingest 2's time.
- Percent is converted to decimal. Change detection uses the unconverted values: values that are equal before dividing by 100 never create a new record.
- Input order doesn't matter. Duplicate `ingest_time` values raise.
- Running the function twice gives equal tables.
- The output passes `RECORD_SCHEMA`, including the existing check that `knowledge_time` is not before `valid_date`.

## 3. Rebuild (`pit build`)

`pit build [--raw data/raw] [--out data/validated]` derives every source and writes `data/validated/<source>.parquet`. It prints one line per source with the record count and the number of restated keys.

**The validated layer is derived, not append-only (D-022).**

- It is rebuilt in full from raw on every run and can be deleted at any time.
- Each file is written to a temp file in the same folder, fsynced, then moved into place with `os.replace`, which atomically replaces the old file.
- Raw stays the only source of truth.
- The `.parquet` file and its quality report are two separate files and are not replaced together atomically. This is acceptable because both are rebuilt by the same command.

**Determinism:** two builds from the same raw give equal tables. The test compares tables, not file bytes, because Parquet writer metadata may include the library version.

## 4. As-of query (`query.py`, D-024)

```python
def get(series: str | Sequence[str], start: date, end: date, asof: datetime,
        *, root: Path = Path("data/validated")) -> pa.Table
def get_from(records: pa.Table, series, start, end, asof) -> pa.Table   # the core, on an in-memory table
```

- **`asof`** must be a time-zone-aware UTC `datetime`. A naive or non-UTC datetime raises `ValueError`; a value of another type, such as a `date`, raises `TypeError` (amended by D-030). A plain date is refused because "as of 2024-01-02" could mean the start or the end of that day. That ambiguity is how off-by-one-day lookahead gets in.
- **`start` and `end`** are inclusive dates. `end < start` raises.
- **Result:** for each `(series_id, valid_date)` in range, the record with the greatest `knowledge_time <= asof`. Columns: `series_id`, `valid_date`, `value`, `knowledge_time`, `knowledge_basis`, `ingest_id`, sorted by `series_id`, `valid_date`.
  - If that record's value is null, the row is returned with a null value: we knew the value was missing.
  - Keys with no visible record are absent from the result.
- **Implementation:** DuckDB, with `QUALIFY row_number() OVER (PARTITION BY series_id, valid_date ORDER BY knowledge_time DESC) = 1` after filtering on `knowledge_time <= asof`.
  - Every connection runs `SET TimeZone = 'UTC'` before any query.
  - `get` reads the Parquet files under `root`; `get_from` queries an Arrow table in memory. The property tests use `get_from`, so they need no files.
- **Return type:** a pyarrow `Table`. Callers who want pandas call `.to_pandas()`. pandas stays at the edges.
- **Unknown series:** an empty table, not an error. The query can't tell "no such series" from "not known yet", and as of an early date both are legitimate answers.

### Tests

- A hand-built record set with a restatement:
  - as of before the first record, nothing is returned;
  - as of between the two records, the old value is returned;
  - as of after both, the new value is returned;
  - as of exactly the restatement's `knowledge_time`, the new value is returned (`<=`).
- A null latest value is returned as null.
- The date range is inclusive.
- Multiple series.
- Naive, non-UTC and plain-date `asof` raise.
- The result is the same whatever the machine's local time zone. The test sets `TZ` to a non-UTC zone.

### Added during implementation: `pit get` (D-030)

`pit get SERIES... --start YYYY-MM-DD --end YYYY-MM-DD --asof <UTC time with Z or +00:00> [--root data/validated]` prints the as-of result, one line per row.

## 5. Property tests (Hypothesis)

Strategy: random sequences of 1 to 6 raw ingests of one source, with:

- 1 or 2 series and up to 8 valid dates;
- values drawn from a set of 3 so that A→B→A sequences are common;
- random absences and nulls;
- strictly increasing UTC ingest times, some before and some after their rows' lag-rule instants.

Properties, each checked at random query times T:

1. **Append-invariance**, for every T at or after the first ingest. Let P be the ingests with `ingest_time <= T`. `get_from(derive(P), ..., T)` equals `get_from(derive(all), ..., T)`. This is the project's key test.
2. **Before the first ingest, everything visible is assumed.** For T before the first ingest, every returned row has `knowledge_basis = "assumed"`.
3. **No leaks.** Every returned row has `knowledge_time <= T`.
4. **Causality.** `derive(P)` is a subset of `derive(all)`: later ingests never change or remove earlier records.
5. **Final state.** As of a time after every ingest, each key's value equals its converted value in the last ingest that contained it.

**Why 1 holds, and why it needs the "at or after the first ingest" condition.**

- Records created by an ingest after T have `knowledge_time = ingest_time > T`, so they are invisible at T.
- Each ingest's records depend only on that ingest and earlier ones. Property 4 checks this directly.

Before the first ingest, though, P is empty, while the first ingest's assumed records can have knowledge times at or before T. For example: August data assumed known on Nov 1, downloaded on Nov 19, queried as of Nov 10. This is what "assumed" means: knowledge placed in the past by a rule, after the fact. Property 2 pins it down. Before the first ingest, the store makes claims only by assumption, and labels them as such.

**Added during implementation (D-032).**

- **6. The query is the as-of rule.** `get_from` equals the rule written as a plain Python loop. Properties 1 to 5 compare the query with itself, so a mistake that affects every call alike passes them; mutation testing showed `<` for `<=` surviving all five.
- **Query times** are drawn from random times and from boundaries: every ingest time and lag-rule instant, and one microsecond either side.
- **Speed.** Running these needed a shared DuckDB database with one session per call (D-031) instead of a new database per call.

## 6. Quality report

**First, a short investigation:** does a maintained calendar library, such as `exchange_calendars` with its `XNYS` calendar, model NYSE sessions back to 1926, including Saturdays before 1952-05-24? Record the answer in the design log. If it doesn't, calendar checks start from the first date the library models correctly, and the gap is a stated limitation. Never fall back to Monday to Friday: the real file has 1,158 Saturday rows.

`pit build` writes `data/validated/<source>.quality.json` next to each table.

**Checks warn; they don't stop the build (D-023).** Schema violations still raise, as before. Each check reports a status (`ok` or `warn`), a count and up to 20 examples:

- **Calendar:** dates not on an NYSE session; NYSE sessions missing from the data, within the covered range.
- **Restatements:** the number of keys with more than one record, by ingest. A sudden jump is the most likely sign that the source re-ran its history.
- **Nulls:** count by series.
- **Outliers:** absolute daily values above 0.25 (25%) in any factor. These should be very rare; the October 1987 crash is the kind of day this flags.
- **Gaps:** the largest gap between consecutive dates, in calendar days.

The build prints one summary line per source when anything warns.

**Outcome (D-033, D-034).**

- **The investigation:** `pandas_market_calendars` matches the real file on every date, including all Saturdays. `exchange_calendars` XNYS has no Saturdays and gets pre-1970 holidays wrong, matching only from 1970.
- **Correction to the outlier rationale above:** 0.25 does *not* flag October 1987 (−17.44%, the worst day in the file). It flags no real day at all. Its job is to catch unit and corruption errors: values left in percent exceed it on 44,972 of 105,352 records.
- **Changes during implementation:**
  - Value checks cover every record, not only the latest.
  - Missing sessions are judged per series.
  - Gaps are reported with status `info` and never warn.
  - The restatement check warns above 1% of an ingest's rows.

## 7. ETF price investigation (no code)

Write `docs/research-etf-prices.md`, covering each candidate free source:

- how it is accessed: URL, API key, rate limits;
- the terms of use as they relate to personal research and to downloading data;
- whether close prices are **unadjusted**;
- whether split and dividend events are available separately;
- how far back the history goes, and for which ETFs.

Facts are checked against current sources and dated; nothing is taken from memory. End with a recommendation and the questions still open. The Week 3 spec builds on this document.

## Acceptance criteria

- `uv run pytest -v`, `ruff check` and `ruff format --check` pass locally and in CI.
- `pit ingest ff` followed by `pit build` produces `data/validated/ken_french.parquet` and `ken_french.quality.json`.
- `get("FF3_DAILY.MKT_RF", date(2024,1,2), date(2024,1,31), asof=<a UTC datetime>)` returns decimal values (for example 0.0025, not 0.25).
- The property tests run in CI under Hypothesis's default profile.
- `docs/design-notes.md` and the README reflect what was built.

## Limitations to carry into the README

- **Pre-ingest history is assumed, not observed.** For Fama-French, everything before the first ingest has an assumed knowledge time, and the values are the current vintage. Any restatement made before the first ingest is invisible.
- **The lag rule is calibrated on one observed publication date.** It is deliberately generous.
- **Withdrawn rows are not modelled.** A row a source stops publishing stays visible with its last value.
- **Restatement detection is limited by ingest frequency.** Two changes between ingests appear as one.
- **The validated layer is replaced, not versioned.** Rebuilding after a rule change makes old query results unreproducible, unless the raw layer and the rule version are recorded alongside them.
