# pitdata

A point-in-time research data store for daily market data: what did we know as of date T?

## Known limitations

### Raw layer
- **One writer at a time.** Two ingests running at once could both treat the same content as new. Nothing locks against this.
- **Knowledge time is only as precise as the ingest schedule.** A value is observed when an ingest first sees it, which may be days after publication. Late is safe, but imprecise.
- **Deleted rows are not recorded.** If a source drops rows in a later file, raw does not mark them as withdrawn.
- **`ingest_time` trusts the machine's clock.**
- **Parser fixes do not rewrite raw.** An ingest keeps the `parsed.parquet` its parser produced; `parser_version` in `meta.json` says which parser that was (D-012).
- **Crash safety on macOS is best effort.** Files are fsynced before the rename (D-013), but macOS `fsync` does not force the drive's cache to disk.

### Ingest
- **No retries.** A network error fails `pit ingest ff`; run it again. Nothing is written on failure.
- **Fixed size caps** of 20 MB per download and 50 MB per extracted file (D-018). A source that legitimately grows past them needs a code change.
- **Strict UTF-8 decoding** (D-017). A source that changes encoding stops the ingest until the code is updated.
- **`meta.json` records the requested URL**, not the final URL after any redirect.

### Query (to come)
- DuckDB shows UTC timestamps in the machine's local time zone unless the connection runs `SET TimeZone = 'UTC'`. The stored values are UTC either way; the query layer must set this so results never appear in local time.

### Data
- **A layout change at the source stops the Fama-French ingest** until the parser is updated. The parser fails on anything unexpected instead of guessing (D-014, D-015).
- **Fama-French history before the first ingest is not true point-in-time.** It is the current vintage, with knowledge time from an assumed lag rule and marked `knowledge_basis = "assumed"`.
