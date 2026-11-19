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
