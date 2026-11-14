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
