# Spec: real-time macro data from the Philadelphia Fed (Week 3)

*Agreed 2026-11-24. Implements decisions D-035 to D-039 in `docs/design-notes.md`. Builds on `docs/spec-validated-layer-and-query.md`. Replaces the README roadmap's plan to use ALFRED (D-035).*

## Why this source

The store has no real revision history yet. Fama-French publishes only its current file, so its history is assumed. The Philadelphia Fed's Real-Time Data Set for Macroeconomists (RTDSM) publishes every past vintage of each variable. With it, `pit get` returns different values as of different dates on real data. This is the store's central claim, shown on real data rather than only in tests.

ALFRED was the original plan. Its terms forbid it (D-035).

## Scope

**In.** Three RTDSM variables, end to end: ingest, derivation, build, query, quality report.

| Variable | File | Observations | Vintages | Units (as published) |
|---|---|---|---|---|
| Housing starts (`HSTARTS`) | `hstartsMvMd.xlsx` | monthly, from 1947:01 | monthly, from 1968:M2 | thousands of units, seasonally adjusted |
| Nonfarm payrolls (`EMPLOY`) | `employMvMd.xlsx` | monthly, from 1939:01 | monthly, from 1964:M12 | thousands of employees, seasonally adjusted |
| Real GDP (`ROUTPUT`) | `routputMvQd.xlsx` | quarterly, from 1947:Q1 | monthly, from 1965:M11 | billions of real dollars, seasonally adjusted |

**Out.**
- Other RTDSM variables. Adding one later takes a file name, a series id and a registry entry, with no new parsing or derivation code.
- First-, second- and third-release files: they are derived from the same vintages.
- Growth rates: values are stored in levels, as published.
- ETF prices: next, after `docs/research-etf-prices.md`.

## Facts this spec relies on (checked 2026-11-23 and 2026-11-24)

**Files and access**
- **URLs:** `https://www.philadelphiafed.org/-/media/FRBP/Assets/Surveys-And-Data/real-time-data/data-files/xlsx/<file>`. They answer HTTP 200 without the `hash` query parameter the site adds.
- **No API key, no stated rate limit.**
- **Updates:** "All data are updated at the end of each month."
- **Layout** of the three files as downloaded on 2026-11-23:
  - one worksheet;
  - row 1 is `DATE`, then one column per vintage, named `<ROOT><yy>M<m>`, for example `HSTARTS68M2` or `ROUTPUT26M11`;
  - column A holds observation dates as text: `1947:01` (monthly) or `1947:Q1` (quarterly);
  - every missing cell is the text `#N/A`; every other cell is a number.
  - Counts: housing starts 957 observations × 705 vintages; payrolls 1,053 × 743; real GDP 319 × 733.
- **Revisions are common:** 810, 1,048 and 318 observations change across vintages. A value that changes and later returns (A→B→A) happens 81, 273 and 2 times. So the D-020 rule matters on real data.
- **`#N/A` after a number:** a number later becomes `#N/A` 265, 96 and 51 times. Vintages cover different spans of history, depending on which historical reports the Philadelphia Fed used for that vintage.

**What a vintage date means**
- **Housing starts and payrolls** (notes on each variable): "the day corresponding to the monthly vintages depends on the day the data are released by government statistical agencies. In other words, we drop the significance of the 15th day." So a vintage dated month M includes that month's release, which can come late in M.
- **Monthly real GDP vintages** (general NIPA notes): "We think of the date as being in the middle of the month, before BEA releases its next report at the end of that month."
- **The source's own caveat:** "in some cases our deep-history report was published on a date after the vintage date." Some old vintages may hold values that were not yet public on their vintage date. This is a limitation we inherit.

**Terms**
- Philadelphia Fed site terms: content "may be used for informational, educational, and research purposes only".
- Nothing on the pages read restricts storing data or using it in software.
- Preferred citation: Croushore and Stark, "A Real-Time Data Set for Macroeconomists," *Journal of Econometrics* 105 (2001), pp. 111–30.

**Excel reading**
- `openpyxl` (the version current on 2026-11-23) fails on `employMvMd.xlsx` while reading the workbook's properties, before any cell.
- `python-calamine` 0.9.1 (MIT) reads all three files. Through pandas 3.1.1's `read_excel(engine="calamine")`, the payrolls file takes 1.4 s.

## Task order

1. **This spec,** and design-log entries D-035 to D-039. README roadmap updated.
2. **Schema:**
   - a `vintage` column in `RAW_SCHEMA`;
   - `vintage` as a third `knowledge_basis`;
   - reading old raw files.
3. **`sources/philly_fed.py`:**
   - `extract_sheet` (xlsx bytes to canonical CSV text);
   - `parse_vintage_matrix` (CSV text to long rows);
   - fixtures.
4. **Ingest and CLI:** `pit ingest rtdsm`.
5. **Derivation:**
   - `VintageRule`;
   - replaying the first ingest's vintages;
   - an updated reference implementation and differential test;
   - property tests extended to vintage sources.
6. **Build and quality:**
   - registry entries and quality configs;
   - an end-to-end check on real data, which becomes the README demo.
7. **Docs:** design log, README, `CLAUDE.md` status.

Explain the approach before writing each component.

## 2. Schema

**Raw gains `vintage`** (`date32`, nullable, D-038).
- It holds the first day of the vintage month the file names: `HSTARTS68M2` becomes 1968-02-01.
- It is what the file said, so it belongs in raw (D-006 holds).
- How a vintage month becomes an instant is decided in the validated layer.
- Fama-French rows have `vintage = null`.

**`RECORD_SCHEMA` gains `vintage` too,** so each record says which vintage it came from.

**`KNOWLEDGE_BASES` gains `"vintage"`** (D-037): the source documents when the value was public, approximated by a rule.

**`SCHEMA_VERSION` becomes `"2"`.**

**Old raw files are never rewritten.**
- `read_ingest` accepts a version 1 `parsed.parquet` (no `vintage` column) and adds an all-null `vintage` column on read.
- Anything else that doesn't match still raises.
- The existing Fama-French ingests keep working unchanged.

**Tests:**
- a version 1 file reads with a null `vintage`;
- a version 2 file round-trips;
- a missing or mistyped column still raises;
- `vintage` is accepted as a basis.

## 3. Parsing (`sources/philly_fed.py`)

**One source per file** (D-038).
- Sources are `rtdsm_hstarts`, `rtdsm_employ` and `rtdsm_routput`.
- "The first ingest" is defined per source (D-021). If the three files shared one source, only the first file downloaded would count as a first ingest, and the other two would lose their vintage history.

**Series ids:** `RTDSM.HSTARTS`, `RTDSM.EMPLOY`, `RTDSM.ROUTPUT`.

**`extract_sheet(original: bytes) -> tuple[str, str]`** returns `(content_name, csv_text)`.
- Reads the workbook with `pandas.read_excel(engine="calamine", header=None, dtype=object)`.
- Requires exactly one worksheet, and writes it as canonical CSV:
  - rows and columns in file order;
  - numbers written with `repr(float)`, which round-trips exactly;
  - text written as is;
  - empty cells written as empty.
- This text is the `content/` file and the input to the content hash (D-007).
- Excel files carry metadata, such as a modified date, that changes the bytes without changing the data.
- Pandas stays at this edge, as for Fama-French.

**`parse_vintage_matrix(text: str, root: str, series_id: str, frequency: "M" | "Q") -> pa.Table`** is a pure function, and fails loudly:
- **Header:** the first cell must be `DATE`. Every other header must match `^<root>(\d{2})M(\d{1,2})$` and be unique.
- **Vintage years:**
  - two-digit years are read as 1960 to 2059;
  - month 1 to 12;
  - vintages must be strictly increasing left to right;
  - a vintage later than the ingest month raises at ingest. That catches a wrong century.
- **Observation dates:**
  - `YYYY:MM` for monthly, `YYYY:Qn` for quarterly;
  - stored as the first day of the period (2026:09 → 2026-09-01; 2026:Q3 → 2026-07-01);
  - strictly increasing; any other form raises.
- **Cells:** a number becomes a row with that value. `#N/A` becomes **no row** (D-038). Anything else raises, including an empty cell, since the files have none.
- **Output:** `series_id`, `valid_date`, `vintage`, `value`, `source`. Only `write_raw` adds the ingest columns (D-009).

**Fixtures:** small CSV texts with invented numbers for the parser. One tiny `.xlsx` for `extract_sheet`, written by a script kept next to it (`tests/fixtures/make_rtdsm_xlsx.py`), so it can be checked and regenerated. Two vintages × four observations, including `#N/A`, an A→B→A cell and a quarterly variant.

## 4. Ingest and CLI

**`ingest_rtdsm(root, ingest_time, variable, *, fetch=download) -> IngestResult`:**
- download, then `extract_sheet`, `parse_vintage_matrix`, `write_raw`;
- `fetch` is injected (D-019);
- size caps as for Fama-French: 20 MB download, 50 MB canonical CSV.

**`pit ingest rtdsm [--variable hstarts|employ|routput]`:**
- with no option, ingests all three, waiting two seconds between downloads;
- prints one line per variable: written, or unchanged.

**Idempotency:** a second run on the same data writes nothing. Proven by a test that feeds the same workbook bytes twice, and by a second fixture workbook with the same cells but a different modified date.

## 5. Derivation (D-039)

**`SourceConfig` gains an optional `vintage_rule`.**
- Exactly one of `lag_rule` and `vintage_rule` is set.
- `VintageRule(name="rtdsm_next_month_start", version="1")` maps vintage month M to **00:00 UTC on the first day of month M+1** (D-036).
- That is after any release in M, so it is never early.
- How late depends on the release day: roughly 2 weeks for housing starts (mid-to-late-month releases), 3 to 4 for payrolls (early-month releases) and up to about 5 for real GDP, whose vintage includes the previous month's release.

**For a vintage source, each ingest becomes one or more snapshots,** and the existing record-per-change rule (D-020) runs over the snapshots in time order.

- **The first ingest of the source** becomes one snapshot per vintage column, at `min(vintage_rule(v), ingest_time)`.
  - Basis is `vintage` when the rule's instant is strictly earlier than `ingest_time`, and `observed` otherwise. Only the newest vintage can be capped.
  - This is D-021 with the lag rule replaced by the vintage date.
- **Each later ingest** becomes **one** snapshot: its newest vintage column only, at `ingest_time`, basis `observed`. This is exactly how a Fama-French ingest behaves.
- **Snapshots of one key must have distinct times.** A tie raises.
- **Absent means unchanged.** A key missing from a snapshot (an `#N/A` cell) produces no record, as for Fama-French.
- **Each record keeps its vintage,** plus the `ingest_time` and `ingest_id` of the ingest it came from.

**Why later ingests use only their newest column.** Older columns in a later download describe the past, which we already recorded. Using them would rewrite what the store said the past knew, which breaks append-invariance.

**The costs:**
- A correction the Philadelphia Fed makes to an old vintage after our first ingest is kept in raw but never applied to the past.
- If ingests are missed, intermediate vintages between two of our downloads collapse into one. This is the same limitation as "two changes between ingests appear as one".

**Rebuilding from scratch is the alternative, and is rejected.** Deriving everything from the latest download each time matches the source's current view. But a rebuild would change past answers, and append-invariance is the property this project exists to prove.

**Tests:**
- the snapshot table for a two-ingest example, written by hand;
- A→B→A across vintages;
- `#N/A` gaps;
- the capped newest vintage;
- a later ingest with a corrected old vintage, which changes nothing;
- a later ingest whose newest value differs, which creates an `observed` record;
- a vintage-month tie, which raises;
- the reference implementation in `tests/test_validated.py` extended the same way, with the differential test run on vintage histories.

**Property tests:** the generator also produces vintage histories, a few vintages per ingest. Properties 0, 1, 3, 4 and 5 apply unchanged. Property 2 becomes "before the first ingest, no visible row is `observed`": rows are `assumed` or `vintage`.

## 6. Build, quality and query

- **Registry:** `CONFIGS` gains the three sources.
- **`QualityConfig` for each:** `calendar=None`, since monthly and quarterly data have no exchange calendar, and `max_abs_value=None`, since levels have no natural bound. Restatement, null and gap checks run as usual.
  - Nulls should be zero, because `#N/A` is absent rather than null.
- **Build provenance (D-027)** records the vintage rule in place of a lag rule.
- **The query is unchanged.**

**Real-data check (becomes the README demo):**
- `pit get RTDSM.ROUTPUT --start 2012-04-01 --end 2012-04-01` at three `--asof` times in late 2012 returns three different levels for 2012 Q2, each labelled `vintage`.
- The documentation's growth-rate example shows Q2 2012 was revised in each of the three months after its first release.

## Acceptance criteria

- `uv run pytest`, `ruff check` and `ruff format --check` pass locally and in CI.
- `pit ingest rtdsm` twice: written, then unchanged, for all three variables.
- `pit build` writes `rtdsm_<variable>.parquet` and `.quality.json` for each, and Fama-French still builds from its existing raw files.
- The real-data check above shows three revisions of 2012 Q2.
- The property tests cover vintage histories.

## Limitations to carry into the README

- **Vintage knowledge times are approximate:** the first day of the following month. Never earlier than the release; typically 2 to 5 weeks later.
- **Some old vintages may contain lookahead from the source itself,** where a historical report was published after its assigned vintage date.
- **Corrections to old vintages after the first ingest are not applied to the past.** They are kept in raw.
- **Delayed releases, such as during the 2013 government shutdown, are gaps,** not dated events.
- **Values are levels in the source's units.** The store keeps no unit metadata beyond the source module and the README.
- **Raw repeats the full vintage history on every download,** about 960,000 rows a month for three variables.
