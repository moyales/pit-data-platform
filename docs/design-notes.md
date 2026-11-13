### D-001: Parquet files queried by DuckDB, not Postgres
- **Why.** Research queries scan a few columns across many rows (all values of one series over ten years). Columnar storage reads only the columns needed and compresses well. DuckDB runs in-process, so "one command rebuilds the store" needs no database server.
- **Alternatives.** Postgres: row-oriented, needs a running server, better at many small concurrent writes, which you do not have. Plain CSV: no types, slow.
- **Limitation.** Single machine, single writer. No concurrent access control. Fine at this scale; it is the first thing to break at 5,000 stocks of minute bars (interview question 5).
### D-002: Raw layer is append-only and never edited
- **Why.** If raw files never change, any result can be rebuilt from them, and a parser bug is fixed by re-running the parser instead of re-downloading (the source may have changed by then).
- **How.** Each ingest writes a new file stamped with ingest time, and keeps the original downloaded bytes next to the parsed Parquet.
- **Limitation.** Storage grows with every ingest, including when most rows are unchanged. Acceptable for megabytes; a real system would deduplicate.
### D-003: Every row carries valid time and knowledge time
- See Step 7. This is the core of the project.
### D-004: Knowledge time for sources with no revision history
- See Step 9. This is the most important limitation in Week 1.
## D-005: Strict schema validation at write time
**Decision.** One pyarrow schema for all sources; validate() rejects any mismatch rather than coercing, except lossless timestamp-unit conversion.
**Why.** Silent coercion (naive times assumed UTC, strings parsed as floats) is how bad data enters unnoticed. A failed ingest is visible; a wrong value is not.
**Alternatives considered.** pandera or Pydantic models: more expressive, but an extra dependency and row-oriented (Pydantic). Per-source schemas: more flexible, but the as-of query would need a branch for each source.
**Limitation.** Validation is opt-in unless the raw writer enforces it. The knowledge-time check is a floor that catches impossible timestamps, not merely optimistic ones.