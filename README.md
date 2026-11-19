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

### Data
- **A layout change at the source stops the Fama-French ingest** until the parser is updated. The parser fails on anything unexpected instead of guessing (D-014, D-015).
- **Fama-French history before the first ingest is not true point-in-time.** It is the current vintage, with knowledge time from an assumed lag rule and marked `knowledge_basis = "assumed"`.
