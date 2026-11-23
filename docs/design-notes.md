# Design notes

One entry per design decision, written when the decision is made. Later entries can amend earlier ones; the amended entry says so.

## D-001: Parquet files queried by DuckDB, not Postgres
**Decision.** Store data as Parquet files and query them in place with DuckDB.
**Why.** Research queries scan a few columns across many rows (all values of one series over ten years). Columnar storage reads only the columns needed and compresses well. DuckDB runs in-process, so "one command rebuilds the store" needs no database server.
**Alternatives considered.** Postgres: row-oriented, needs a running server, better at many small concurrent writes, which this project does not have. Plain CSV: no types, slow.
**Limitation.** Single machine, single writer, no concurrent access control. Fine at this scale; it is the first thing to break at thousands of stocks of minute bars.

## D-002: Raw layer is append-only and never edited
**Decision.** Each ingest writes a new, immutable folder holding the original downloaded bytes next to the parsed Parquet. Nothing under `data/raw/` is ever edited or deleted.
**Why.** If raw files never change, any result can be rebuilt from them, and a parser bug is fixed by re-running the parser instead of re-downloading (the source may have changed by then).
**Alternatives considered.** Overwriting a single "latest" file per source: simpler, but destroys the revision history that point-in-time queries depend on.
**Limitation.** Storage grows with every ingest that changes anything, even when most rows are unchanged. Acceptable for megabytes; a real system would deduplicate at the row level.

## D-003: Every record carries valid time and knowledge time
**Decision.** Each record has `valid_date` (the period the value describes) and `knowledge_time` (when the value could first have been known, UTC). A record is visible as of T if and only if `knowledge_time <= T`.
**Why.** Joining on valid time alone uses numbers nobody had yet (lookahead bias), the most common reason a backtest beats live trading. Storing both timestamps (bitemporal modeling) lets an as-of query reproduce what was known on any date.
**Alternatives considered.** Valid time only, with a fixed publication lag applied at query time: simpler, but cannot represent revisions, so a restated value silently replaces history.
**Limitation.** `knowledge_time` is only as good as its evidence. For sources without published vintages it is partly assumed; see D-004. Amended by D-006: knowledge time is derived in the validated layer, not stored in raw.

## D-004: Knowledge time for sources with no revision history
**Decision.** For sources that publish only their current version (Fama-French), history present in the first ingest gets `knowledge_time` from a conservative, documented lag rule, marked `knowledge_basis = "assumed"`. Values that first appear or change in a later ingest get that ingest's time, marked `"observed"`.
**Why.** Stamping backfilled history with the first ingest time would be honest but would make every historical as-of query empty. A lag rule keeps history usable, and `knowledge_basis` keeps the approximation visible in the data instead of buried in code.
**Alternatives considered.** Treating the current file as if it had always been known: hides lookahead. Excluding pre-ingest history: loses all backtest value.
**Limitation.** Before the first ingest, the store is a current-vintage history with an assumed lag, not true point-in-time; restated values are already baked in. Real vintages exist only for ALFRED macro series. Amended by D-006: this rule runs in the validated layer.

## D-005: Strict schema validation at write time
**Decision.** One pyarrow schema shape for all sources; `validate()` rejects any mismatch rather than coercing, except lossless timestamp-unit conversion.
**Why.** Silent coercion (naive times assumed UTC, strings parsed as floats) is how bad data enters unnoticed. A failed ingest is visible; a wrong value is not.
**Alternatives considered.** pandera or Pydantic models: more expressive, but an extra dependency and row-oriented (Pydantic). Per-source schemas: more flexible, but the as-of query would need a branch for each source.
**Limitation.** Validation is opt-in unless the writer enforces it (addressed by D-009). The knowledge-time check is a floor that catches impossible timestamps, not merely optimistic ones.

## D-006: Raw stores observations; knowledge time is derived
**Decision.** Raw records hold only what was observed: `series_id`, `valid_date`, `value`, `source`, `ingest_time`, `ingest_id` (`RAW_SCHEMA`). `knowledge_time` and `knowledge_basis` are computed in the validated layer by replaying a source's raw ingests in time order (`RECORD_SCHEMA`).
**Why.** Knowledge time is an inference ("first seen in ingest X", or "assumed by the lag rule"), not a fact any single file states. Kept out of raw, it becomes a deterministic function of immutable inputs: rebuildable, testable, and the lag rule can change without re-downloading.
**Alternatives considered.** Stamping knowledge time at ingest (the original plan): one schema and less work this week, but it bakes the lag rule into files that may never be edited.
**Limitation.** Two schemas to maintain, and the validated layer must replay all ingests of a source to rebuild. Cheap at this data size.

## D-007: Idempotency by hashing the extracted content
**Decision.** An ingest is skipped when the SHA-256 of the extracted payload (for example the CSV inside a zip) matches an earlier ingest of the same source. The original download is kept but not used for the comparison.
**Why.** Archives store member timestamps, so a source that rebuilds a zip without changing a number produces different bytes. Idempotency should be about the data, not the packaging.
**Alternatives considered.** Hashing the download: records spurious "new" ingests. Comparing by date: a source can change twice in a day or not for a month.
**Limitation.** A change that only affects formatting (whitespace, line endings) still counts as a new ingest. Harmless: the replay finds no changed values.

## D-008: One self-describing folder per ingest, renamed into place
**Decision.** Each ingest is written to a temporary folder inside `data/raw/<source>/`, including its own `meta.json`, then renamed to its final name. There is no shared manifest; the ingest index is a scan of the `meta.json` files.
**Why.** A rename within one filesystem is atomic, so readers see a whole ingest or none of it. Keeping metadata inside the folder removes the failure where data is written but a separate manifest append is not.
**Alternatives considered.** A shared manifest file appended after each write: two writes that can fall out of step on a crash.
**Limitation.** The atomicity guarantee holds only within one filesystem. Finding a hash means scanning folders, which is fine for hundreds of ingests. Concurrent writers are not handled.

## D-009: The writer stamps ingest metadata and enforces validation
**Decision.** Parsers return `series_id`, `valid_date`, `value`, `source`. `write_raw` adds `ingest_time` and `ingest_id` and calls `validate(table, RAW_SCHEMA)` before anything is written.
**Why.** One authority for ingest metadata, and validation at the one point every write passes through, so no code path can skip it.
**Alternatives considered.** Each parser validates its own output: easy to forget in the next parser.
**Limitation.** None significant; parsers must not emit ingest columns.

## D-010: The clock is injected
**Decision.** Every function that needs the current time takes it as an argument. Only the CLI reads the real clock.
**Why.** Tests can use fixed times and replay sequences of ingests deterministically, which the knowledge-time derivation and the append-invariance property test both depend on.
**Alternatives considered.** Calling `datetime.now()` where needed and patching it in tests: works, but hides the dependency.
**Limitation.** `ingest_time` trusts the machine's clock.

## D-011: Parsers are pure functions; input/output stays at the edges
**Decision.** Each source module separates `download` (network), `extract` (archive handling) and `parse` (text in, table out). Parsers locate headers by content, fail loudly on anything unexpected, and never guess. No retries in v1.
**Why.** The risky logic lives in the pure parser, which is fully testable with small hand-made fixtures and no network. Finding the header by content survives a preamble that gains a line.
**Alternatives considered.** `pandas-datareader` or `read_csv` with a fixed `skiprows`: shorter, but hides exactly the details the project needs to control, and breaks silently when the layout shifts.
**Limitation.** A format change on the source's side stops the ingest until the parser is updated. That is intended: failing is better than parsing wrongly. A transient network error also fails the command; re-run it.

## D-012: Each ingest records the parser version
**Decision.** `meta.json` has a `parser_version` field, which the source module passes to `write_raw` and bumps whenever the parser's output could change.
**Why.** Raw is never rewritten, and identical content returns `unchanged`. So after a parser fix, old ingests keep the old `parsed.parquet`. Without the version, nothing shows which ingests came from which parser.
**Alternatives considered.** Re-parsing and overwriting `parsed.parquet`: breaks D-002. Using `pitdata_version` only: it changes for unrelated reasons and nobody bumps it per parser.
**Limitation.** It is a hand-maintained string; forgetting to bump it is not caught. Parser fixes reach the data only when the validated layer re-parses `content/`, not by re-ingesting.

## D-013: Files are fsynced before the ingest folder is renamed
**Decision.** `write_raw` fsyncs every file and folder in the temp folder before the rename, and the source folder after it.
**Why.** The rename alone protects against the program crashing. After a power loss, the operating system can have saved the rename before the file contents, leaving a complete-looking folder with truncated files. The fsyncs close that gap for a few milliseconds per ingest.
**Alternatives considered.** No fsync, with "all-or-nothing on program crash only" in the README: simpler, but weakens the main guarantee of the raw layer.
**Limitation.** On macOS, `fsync` hands data to the drive but does not force the drive's own cache to disk (that needs `F_FULLFSYNC`). A power loss there can still, rarely, lose the last ingest's data.

## D-014: A source table ends at the first blank line, and every line before it must be a data row
**Decision.** `parse_ff3_daily` reads the lines between the header and the first blank line (or the end of the text). Each must be a valid data row, or parsing raises with the line number. Lines after the blank line are ignored. The header must appear exactly once.
**Why.** The alternative rule, "stop at the first line that does not look like data", turns a corrupted row in the middle of the file into a silent truncation: every later row disappears and nothing fails. With a blank-line boundary, a bad row can only raise. The real file (checked 2026-11-18) has one blank line between the last row and the copyright notice.
**Alternatives considered.** Stop at the first non-data line: tolerant of layout changes, but loses data silently. Read a fixed number of rows: breaks on every update.
**Limitation.** If the source drops the blank line before the copyright notice, the ingest fails until the parser is updated. That is intended. The "header exactly once" rule fits the daily file only: monthly files repeat the same header for a second, annual table, so a monthly parser will need its own rule.

## D-015: Values must match a plain decimal pattern before conversion
**Decision.** Each value must match `-?digits(.digits)?` before it is converted with `float()`. Dates must be exactly eight digits and a real calendar date, checked with a regex and `date()`, not `strptime`.
**Why.** `float()` accepts `"nan"`, `"inf"`, `"1e-3"` and `"1_000"`, and `strptime("%Y%m%d")` accepts unpadded dates such as `"2024112"`, which is ambiguous (Jan 12 or Nov 2?). None of these appear in the real file, so if one does, the layout has changed and the parser should fail, not guess.
**Alternatives considered.** Plain `float()` and `strptime`: shorter, but each accepts inputs that are silently wrong.
**Limitation.** Formats the source might legitimately adopt later (more decimals are fine; a leading `+` or scientific notation are not) need a parser change and a `PARSER_VERSION` bump.

## D-016: Fixtures use the real layout with invented numbers, stored byte-exact
**Decision.** `tests/fixtures/ff3_daily_sample.csv` copies the real file's layout (preamble, header, padding, CRLF line endings, blank line, copyright line) but its wording and all numbers are invented. `.gitattributes` marks `tests/fixtures/*` as `-text`, so git never converts their line endings, and a test checks that the CRLF endings are still there.
**Why.** Tests must exercise the real format, including Windows line endings, without committing any vendor data. A git setting such as `core.autocrlf` could otherwise silently turn the fixture into an LF file, and the tests would stop covering the real case while still passing.
**Alternatives considered.** A few real rows: closer to the source, but it is vendor data in the repo. Building all variants in code: no fixture to drift, but no byte-exact copy of the real layout.
**Limitation.** The fixture matches the layout as checked on 2026-11-18; if the source changes, only the real ingest will notice.

## D-017: Downloaded text is decoded as strict UTF-8
**Decision.** `decode()` reads the extracted CSV as UTF-8 with strict error handling, and raises `ParseError` on bytes that are not valid UTF-8.
**Why.** The real file is plain ASCII, which UTF-8 reads unchanged, and UTF-8 also accepts a harmless addition such as `©`. Strict mode never guesses: a file that switches to Windows-1252 raises instead of producing wrong text. The parser's own checks already protect the data rows, so decoding only has to avoid guessing.
**Alternatives considered.** Strict ASCII: matches today exactly, but stops the ingest over a symbol in the copyright line. Latin-1: can never fail, so it can silently turn bytes into the wrong characters.
**Limitation.** A source that moves to another encoding stops the ingest until the code is changed. That is intended.

## D-018: Size caps on download and extraction, enforced while reading
**Decision.** `download` streams the response and raises past 20 MB; `extract_csv` reads at most 50 MB plus one byte from the zip member and raises if it got more. Both are about 50 times the real sizes (a 180 KB zip holding a 1.2 MB CSV).
**Why.** Everything is held in memory because `write_raw` takes bytes. A zip can be built to expand from kilobytes to gigabytes, and the sizes declared by HTTP headers and zip headers can be missing or false, so the only trustworthy limit is counting what actually arrives.
**Alternatives considered.** No caps: one bad response can exhaust memory. Trusting `Content-Length` or the zip's declared size: both can be faked.
**Limitation.** The caps are fixed numbers. A source that legitimately grows past them needs a code change.

## D-019: The download function is injected, like the clock
**Decision.** `ingest_ff3_daily(root, ingest_time, *, fetch=download)` takes the download function as an optional argument. Tests pass a function returning fixture bytes; the CLI uses the default.
**Why.** Same reasoning as D-010: an argument makes the dependency visible and lets tests run the full download-to-disk path without a network or patching module internals.
**Alternatives considered.** Monkeypatching `download` in tests (the spec's plan): works, but hides the dependency. It also does not reach the default argument, which is bound when the function is defined, so the CLI tests would still need to wrap the function.
**Limitation.** One more parameter on a public function. `download` itself is tested with a fake `requests.get`; the real network path is checked by hand.

## D-020: Knowledge-time records are created per change between consecutive ingests
**Decision.** For each `(series_id, valid_date)`, the validated layer creates a record when the key first appears and whenever its value differs (`IS DISTINCT FROM`) from its value in the previous ingest that contained it. Values are compared as parsed, before unit conversion. This amends the Week 1 spec's rule, "each distinct value gets one record".
**Why.** The old rule loses the order of events. A value that goes A, then B, then back to A gives records A@1 and B@2, so a query as of ingest 3 returns B, which is wrong. A timeline needs change points, not distinct values. Comparing before conversion keeps equality exact for values parsed from fixed-decimal text.
**Alternatives considered.** One record per distinct value (the original rule): wrong on reversions. One record per ingest per key: correct, but stores every unchanged value again, about 105,000 rows per Fama-French ingest.
**Limitation.** Two changes between consecutive ingests look like one. A key missing from a later ingest creates no record, so withdrawals are not modelled.

## D-021: Assumed knowledge time: end of the second month after, capped by the first ingest's time
**Decision.** Records from a source's first ingest get `knowledge_time = min(rule(valid_date), ingest_time)`, with `knowledge_basis = "assumed"` only when the rule's instant is strictly earlier. For Fama-French the rule is 00:00 UTC on the first day of the third month after the valid month (for September data, December 1), named and versioned as `ff_month_end_plus_2` v1. All later records get their ingest's time and are `observed`.
**Why.** The one publication date observed (the 2026-10-26 file, covering data through September 30) shows a lag of about 4 weeks after month end. The rule allows about 9 weeks, because one observation is thin evidence and too late is safe while too early is lookahead. The cap uses the better evidence when it exists: if we downloaded the file before the rule's date, we really did know the values then.
**Alternatives considered.** End of the following month (October 31 for September data): only 5 days of margin over the one observed date. Stamping all backfilled history with the first ingest time: honest, but makes every historical as-of query empty.
**Limitation.** The rule is a guess calibrated on one data point. The newest month or two before the first ingest is unusable in a backtest. Values are the current vintage, so restatements made before the first ingest are invisible.

## D-022: The validated layer is derived and replaced, not appended
**Decision.** `pit build` derives every source from raw in full and writes `data/validated/<source>.parquet`. Each file goes to a temp file in the same folder, is fsynced, then replaces the old file with `os.replace`. Raw stays the only source of truth.
**Why.** Derivation is a pure, deterministic function of raw, so a full rebuild is always correct and lets the lag rule or a parser change take effect without re-downloading. `os.replace` is atomic within one filesystem, so a reader sees the old file or the new one, never a partial one.
**Alternatives considered.** Appending new records per ingest: faster, but a rule change would need a migration, and an append bug would persist. Keeping every build: reproducible, but adds versioning machinery before it is needed.
**Limitation.** After a rule change, results from the previous build cannot be reproduced unless the raw state and the rule version are recorded with them. The table and its quality report are separate files and are not replaced together atomically.

## D-023: Quality checks warn and write a report; only schema violations stop a build
**Decision.** `pit build` writes `<source>.quality.json` with checks: calendar sessions, restatement counts, nulls, outliers, gaps. Each check has a status (`ok`/`warn`), a count and examples. Warnings are printed but do not fail the build. Schema validation still raises.
**Why.** A failing quality check is a question for a person, not proof the data is wrong: a missing day may be a real market closure the calendar does not know. Stopping the build would block all data over one questionable date. Schema violations are different: they mean the code is wrong.
**Alternatives considered.** Fail on any check: safest in principle, but the first odd historical date blocks everything. Store a quality flag on each record: puts check results into the record schema and the as-of query, mixing data with opinions about the data.
**Limitation.** Warnings can be ignored. Nothing stops a backtest from using data with an open warning.

## D-024: The as-of query takes an aware UTC datetime and returns an Arrow table
**Decision.** `get(series, start, end, asof)` requires `asof` to be a time-zone-aware UTC `datetime`, and returns a pyarrow `Table` with, per key, the record with the greatest `knowledge_time <= asof`. The core, `get_from`, runs on an in-memory table. DuckDB connections set `TimeZone = 'UTC'`.
**Why.** A plain date is ambiguous (the start or the end of the day?), and that ambiguity is how off-by-one-day lookahead enters. Arrow keeps pandas at the edges and converts to pandas in one call. Running the core on an in-memory table makes the property tests fast and file-free. DuckDB otherwise displays timestamps in the machine's local zone.
**Alternatives considered.** Accepting dates as end of day UTC: convenient, but silently picks a convention. Returning pandas: familiar, but makes pandas part of the storage contract.
**Limitation.** Callers must build a UTC datetime for every query. An unknown series returns an empty table, not an error, because "no such series" and "not known yet" look the same.

## D-025: Derivation is one DuckDB window query, checked against a Python reference implementation
**Decision.** `derive_records` runs one SQL query: per key, ordered by ingest time, keep rows where `row_number() = 1 OR value IS DISTINCT FROM lag(value)`. The tests contain an independent Python implementation written directly from the spec, and a Hypothesis test checks that both produce identical records on random sequences of ingests (differential testing). The query runs on a private in-memory connection with `TimeZone = 'UTC'` (since D-031, a private session on a shared in-memory database).
**Why.** The query states the rule in one place and stays fast as ingests accumulate, while the Python loop is the easiest version to check against the spec. Two implementations written differently rarely share a bug, so a disagreement points directly at one of them. Two SQL traps are guarded explicitly. Without `row_number() = 1`, a key whose first value is null gets no record, because `lag()` is also null and `NULL IS DISTINCT FROM NULL` is false. Without the UTC setting, the lag rule's instant is midnight in the machine's local zone: nine hours early in Tokyo, which is lookahead.
**Alternatives considered.** Python loop only: readable, but about 105,000 iterations per ingest in pure Python. SQL only: fast, but nothing independent checks that the SQL means what the spec says.
**Limitation.** Two implementations to keep in step when the rule changes. The differential test only covers the cases its generator produces (two series, four dates, four values).

## D-026: Each source declares its validation config in its own module
**Decision.** A `SourceConfig(lag_rule, divide_by)` is declared next to each source's parser (for Fama-French, `fama_french.VALIDATION`). `validated.py` is generic, and its signature is `derive_records(ingests, config)` instead of the spec's `derive_records(ingests, rule)`. Conversion is a division by an exact number (`divide_by=100.0`), not a multiplication by `0.01`.
**Why.** The publication lag and the units are facts about the source, and they belong next to the parser that knows the file. New sources (ETF prices, ALFRED) bring their own config without changing the validated layer. Dividing by 100 rounds once; `0.01` has no exact binary form, so multiplying by it rounds twice.
**Alternatives considered.** A registry of sources inside `validated.py`: centralises knowledge about every source in one module. Passing the rule and the conversion as separate arguments: works, but every caller has to keep them paired.
**Limitation.** One lag rule and one conversion per source, applied to every series in it. A source whose series publish on different schedules will need a per-series rule.

## D-027: Build provenance is stored inside each validated file's Parquet metadata
**Decision.** `pit build` writes a `pitdata_build` key into each validated file's Parquet key-value metadata, holding the source, the lag rule (name, version, months), `divide_by`, the raw ingest ids in time order, the pitdata version, and the record and restated-key counts. No build time is recorded. `read_build_info(path)` returns it; DuckDB can read it with `parquet_kv_metadata`.
**Why.** A validated file means nothing without knowing which rule and which raw ingests produced it. Stored inside the file, the provenance is replaced in the same atomic step as the data, so the two can never disagree. These inputs fully determine the content, so a build time would add no explanation and would make builds non-deterministic.
**Alternatives considered.** A sidecar `<source>.build.json`: easier to open by hand, but a second file that a crash or a manual copy can separate from the data. Recording the build time: needs the clock injected into the build and makes two builds of the same raw data differ.
**Limitation.** The provenance is invisible without a Parquet-aware tool. The list of ingest ids grows by one per changed ingest, which is a few kilobytes even after years of daily ingests.

## D-028: A raw source with no registered config fails the build
**Decision.** `pitdata/sources/__init__.py` lists every source's `SourceConfig` in `CONFIGS`. If any folder under `data/raw/` is not in it, `pit build` raises before writing anything. `pit build --source <name>` builds one source regardless of the others.
**Why.** A raw folder the code does not know about is a mismatch between data and code, and the project's rule is to fail loudly on mismatches. Checking every source before writing means a failed build changes nothing.
**Alternatives considered.** Skipping unknown folders with a warning: the other sources still build, but a missing config can go unnoticed for a long time.
**Limitation.** One unregistered folder blocks a full build until it is registered or `--source` is used.

## D-029: The as-of query filters first, picks one row with row_number(), and shares its SQL between files and memory
**Decision.** `get` and `get_from` run the same SQL: `WHERE knowledge_time <= $asof` (plus series and date filters), then `QUALIFY row_number() OVER (PARTITION BY series_id, valid_date ORDER BY knowledge_time DESC) = 1`. `get_from` registers an in-memory table as `records`; `get` defines `records` over `read_parquet` of the validated files, listed in Python (hidden and temp files skipped) and schema-checked from their metadata. Two records for one key at the same knowledge time raise. All inputs are bound as parameters.
**Why.** Filtering before picking is the as-of rule itself; picking the latest record first and filtering afterwards loses any key restated after T. `row_number()` selects a whole row, so a null latest value is returned as null; mutation testing confirmed that `arg_max(value, knowledge_time)` returns the older non-null value instead. Sharing the SQL text means the property tests, which use `get_from`, check exactly what real queries run, and DuckDB reads only the parts of each file it needs. (Each query runs in its own DuckDB session; see D-031.)
**Alternatives considered.** Loading every validated file into Arrow and calling `get_from`: one code path, but every query reads everything. A `GROUP BY` with `arg_max`: shorter, but wrong for null latest values. A series-to-source mapping to choose files: one more list to keep in sync.
**Limitation.** Every query lists and schema-checks all validated files, which is cheap for a few sources but grows with their number. The tie check is an extra scan over the selected range.

## D-030: `pit get` prints as-of results and requires an explicit UTC cut-off
**Decision.** `pit get SERIES... --start --end --asof [--root]` prints one line per result (valid date, series, value, knowledge time, basis) and a count. `--asof` is parsed with `datetime.fromisoformat` and must carry an explicit UTC zone (`Z` or `+00:00`); a time without a zone, a bare date, or another zone is rejected. Across the code, a value of the wrong type (such as a `date` for `asof`) raises `TypeError` and a naive or non-UTC datetime raises `ValueError`, through one helper, `schema.require_utc`.
**Why.** The command makes the core idea demonstrable in one line without opening Python. Requiring the zone at the command line applies D-024's rule where people type times: a cut-off is never interpreted in local time. `TypeError` versus `ValueError` follows Python's convention and is clearer than the spec's blanket `ValueError`.
**Alternatives considered.** Accepting a date for `--asof` as end of day UTC: convenient, but silently picks a convention. No CLI: querying only from Python.
**Limitation.** Output is plain text for reading, not for parsing; programs should call `get` and receive an Arrow table.

## D-031: One shared in-memory DuckDB database per process, one session per call
**Decision.** `pitdata/_duckdb.py` opens one in-memory DuckDB database per process. `derive_records` and the query each take a `session()`: a `cursor()` on that database with `TimeZone = 'UTC'`, closed when the call ends. A process that finds a database opened by its parent (after `fork`) opens its own. This replaces the per-call `duckdb.connect()` of D-025 and D-029.
**Why.** Opening a new DuckDB database costs about 25 ms; a cursor costs about 0.01 ms. The property tests make thousands of calls, and a backtest asks one as-of question per rebalance date, so per-call databases cost minutes. A cursor is still a separate connection with its own session: tests confirm that a setting or a registered table in one session is invisible to every other session, including concurrent ones in other threads, so the isolation D-025 asked for is kept.
**Alternatives considered.** Keeping a new database per call: simplest isolation, but the full suite took over two minutes. Making only the tests lighter (fewer examples, fewer query times): about 35 seconds and a weaker search. Marking property tests as slow and skipping them locally: the project's key test would usually not run.
**Limitation.** Correctness relies on DuckDB keeping sessions separate; `tests/test_duckdb_sessions.py` fails if an upgrade changes that. One small in-memory database lives as long as the process. A `SET` without a scope is session-level for `TimeZone` today; a future setting with global default scope would need `SET SESSION`.

## D-032: Property tests check the promises at boundary query times, against plain-Python oracles
**Decision.** `tests/test_properties.py` checks the spec's five properties plus a sixth, "the query is the as-of rule": `get_from` must equal the rule written as a plain Python loop. Each random history (shared generator in `tests/strategies.py`) is checked at one to four query times T, drawn from random times and from boundaries: every ingest time and lag-rule instant, and one microsecond either side. Hypothesis runs 200 examples per test by default; `HYPOTHESIS_PROFILE=thorough` runs 5,000 (`tests/conftest.py`). `hypothesis.event()` labels the cases that matter, such as a key restated after T or an A→B→A sequence, so their frequency can be checked.
**Why.** Off-by-one mistakes (`<` for `<=`, T equal to an ingest time) only show at boundaries, which uniformly random times almost never hit. Properties 1 to 5 compare the query with itself, so a mistake that affects every call alike passes them: mutation testing showed `<` for `<=` surviving all five, which is why the sixth property exists. Writing expected answers in plain Python keeps them independent of the SQL under test.
**Alternatives considered.** Uniformly random T only: misses boundaries. Running the properties through `pit build` and `get` on files: about ten times slower, and file reading is already tested in `test_query.py`. Always running thousands of examples: slows every `pytest` run and CI.
**Limitation.** Testing, not proof: a small world (two series, four dates, up to six ingests, four values) searched thoroughly, on the bet that bugs show up in small cases. The properties check that the store keeps its own promises given the lag rule; they cannot check that the lag rule matches when a source really published. The property tests do not exercise `get`'s file listing.
