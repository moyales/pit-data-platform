# pitdata

[![CI](https://github.com/moyales/pit-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/moyales/pit-data-platform/actions/workflows/ci.yml)

A point-in-time research data store for daily market data. It answers one question: **what did we know as of date T?**

A backtest that uses numbers nobody had yet (a GDP figure before it was published, a factor return after it was restated) looks better than anything you could have traded. This is lookahead bias. pitdata stores every value with *when it could first have been known*, so a query as of any past date sees only what was knowable then.

> **Status: work in progress.** Fama-French works end to end: ingest, knowledge-time derivation and as-of queries. The append-invariance property tests and quality checks are next. See [Roadmap](#roadmap).

## The idea in three sentences

Every value carries two timestamps: `valid_date`, the period it describes, and `knowledge_time` (UTC), when it could first have been known. A value is visible as of T if and only if `knowledge_time <= T`, and a restated value is a new row with a later knowledge time, never an overwrite. The key guarantee, to be tested directly with property tests (next on the roadmap): adding data learned after T never changes an answer as of T.

This is called *bitemporal* modelling. Every record also says whether its knowledge time was **observed** (seen at or near publication) or **assumed** (set by a documented lag rule for history loaded after the fact), so approximations stay visible in the data.

## What works today

```console
$ uv run pit ingest ff
written 105,352 rows (ingest 20261123T213237Z_3f7910502dd6)
$ uv run pit ingest ff
unchanged, matches ingest 20261123T213237Z_3f7910502dd6
$ uv run pit build
ken_french: 105,352 records, 0 restated keys, from 1 ingest -> data/validated/ken_french.parquet
```

Then ask what was known about the market factor around the end of August 2026, at three points in time:

```console
$ uv run pit get FF3_DAILY.MKT_RF --start 2026-08-28 --end 2026-09-02 --asof 2026-09-01T00:00Z
0 rows known as of 2026-09-01T00:00Z

$ uv run pit get FF3_DAILY.MKT_RF --start 2026-08-28 --end 2026-09-02 --asof 2026-11-01T00:00Z
2026-08-28  FF3_DAILY.MKT_RF            -0.0034  2026-11-01T00:00:00Z  assumed
2026-08-31  FF3_DAILY.MKT_RF            -0.0033  2026-11-01T00:00:00Z  assumed
2 rows known as of 2026-11-01T00:00Z

$ uv run pit get FF3_DAILY.MKT_RF --start 2026-08-28 --end 2026-09-02 --asof 2026-12-01T00:00Z
2026-08-28  FF3_DAILY.MKT_RF            -0.0034  2026-11-01T00:00:00Z  assumed
2026-08-31  FF3_DAILY.MKT_RF            -0.0033  2026-11-01T00:00:00Z  assumed
2026-09-01  FF3_DAILY.MKT_RF            -0.0081  2026-11-23T21:32:37Z  observed
2026-09-02  FF3_DAILY.MKT_RF              0.005  2026-11-23T21:32:37Z  observed
4 rows known as of 2026-12-01T00:00Z
```

August's values were in the first download, so their knowledge time comes from a conservative lag rule and is marked `assumed`. September's values were downloaded before the rule's date, so the download time itself is used and marked `observed`. Values are decimals (−0.0034 is −0.34%).

From Python, `pitdata.query.get(series, start, end, asof)` returns the same result as an Arrow table; `asof` must be a time-zone-aware UTC `datetime`.

- **Fama-French daily three factors** (Mkt-RF, SMB, HML, RF, 1926 onwards) downloaded from the Kenneth R. French Data Library and parsed into long format.
- **An append-only raw layer.** Each ingest is one immutable folder holding the download byte for byte, the extracted file, the parsed table and a `meta.json`:

  ```
  data/raw/ken_french/20261119T044133Z_3f7910502dd6/
      original/F-F_Research_Data_Factors_daily_CSV.zip
      content/F-F_Research_Data_Factors_daily.csv
      parsed.parquet
      meta.json
  ```

- **Idempotent ingest.** A re-run with unchanged data writes nothing, because the extracted content is compared by SHA-256 hash. The zip itself is not compared: zips store timestamps, so identical data can arrive in different bytes.
- **All-or-nothing writes.** Files are written to a temporary folder, flushed to disk, then renamed into place in one step.
- **Strict validation.** One schema module, no silent coercion. Naive timestamps, unexpected headers, malformed numbers and corrupted archives raise errors.
- **Queryable with DuckDB as-is:**

  ```sql
  SELECT * FROM read_json('data/raw/*/*/meta.json');                          -- every ingest
  SELECT * FROM 'data/raw/ken_french/*/parsed.parquet' WHERE series_id = 'FF3_DAILY.HML';
  ```

## Quick start

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/moyales/pit-data-platform.git
cd pit-data-platform
uv sync
uv run pit ingest ff                # writes to data/raw/ (git-ignored)
uv run pit build                    # derives data/validated/ from data/raw/
uv run pit get FF3_DAILY.MKT_RF --start 2024-01-02 --end 2024-01-31 --asof 2026-12-01T00:00Z
uv run pytest                       # no test touches the network
```

## How it is built

| Layer | Holds | Status |
|---|---|---|
| **Raw** | What each source file said and when it was fetched. Never edited. | Done |
| **Validated** | Typed records with derived `knowledge_time`, unit conversions (e.g. percent to decimal) and quality checks. Rebuilt from raw at any time. | Done, except quality checks |
| **Query** | `get(series, start, end, asof)` over Parquet with DuckDB. | Done; property tests next |

Some design choices worth knowing:

- **Raw stores observations only.** Knowledge time is an inference ("first seen in ingest X", or "assumed by a lag rule"). It is computed in the validated layer by replaying ingests in order, so the rule can change without re-downloading anything.
- **Parsers are pure functions** (text in, table out), tested with small hand-made fixtures. Download and archive handling stay thin.
- **The clock and the downloader are passed in as arguments.** Tests replay exact sequences of ingests without a network.

Every decision, with alternatives and limitations, is in [`docs/design-notes.md`](docs/design-notes.md). Specifications: [`docs/spec-raw-layer-and-ff-ingest.md`](docs/spec-raw-layer-and-ff-ingest.md) (raw layer and ingest) and [`docs/spec-validated-layer-and-query.md`](docs/spec-validated-layer-and-query.md) (validated layer and query, current).

## Roadmap

- **Property tests (Hypothesis)** for append-invariance, no leaks and causality over random sequences of ingests.
- **Data-quality checks** against a historical exchange calendar.
- **Daily ETF prices**, stored unadjusted, with split and dividend factors kept separately and adjusted prices computed as of a date.
- **Macro series with real revision history** (ALFRED), where knowledge times are observed vintages rather than assumptions.

Out of scope for v1: intraday data, fundamentals, a web UI, cloud deployment and scheduling.

## Data sources and terms

pitdata ships **download code, not data**. Nothing under `data/` is committed, and test fixtures contain only invented numbers.

- **Fama-French factors:** [Kenneth R. French Data Library](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html). Data is fetched by the person running the tool. Please read and respect the library's terms of use.

## Known limitations

This is a personal research project built on free data. Free data has no accuracy guarantee or service level, and sources can change format or disappear. Professional point-in-time datasets are licensed and expensive.

### Scope
- **Single machine, single user, daily frequency.** No concurrency control, scheduling, monitoring or access control.
- **Real vintages exist only for macro data (planned via ALFRED).** For factors and prices, knowledge times for history loaded after the fact are assumed.

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

### Validated layer
- **Rebuilt in full, not versioned.** `pit build` replaces `data/validated/<source>.parquet` each time. Each file records the lag rule, conversion and raw ingest ids that produced it (D-027), but results from before a rule change cannot be reproduced unless the old file was kept.
- **One unregistered raw folder blocks a full build** (D-028). Use `pit build --source <name>` to build the others.

### Query
- **`asof` must be an explicit UTC time.** A date, a time without a zone, or another zone is refused rather than interpreted (D-024, D-030).
- **An unknown series returns no rows, not an error.** "No such series" and "not known yet" look the same to the query.
- **Every query lists and schema-checks all validated files.** Cheap for a few sources; it grows with their number (D-029).

### Data
- **A layout change at the source stops the Fama-French ingest** until the parser is updated. The parser fails on anything unexpected instead of guessing (D-014, D-015).
- **Fama-French history before the first ingest is not true point-in-time.** It is the current vintage, with knowledge time from an assumed lag rule and marked `knowledge_basis = "assumed"`. Restatements made before the first ingest are invisible.
- **Early Fama-French data includes Saturdays** (the NYSE traded on Saturdays until 1952). Calendar checks must use a historical exchange calendar, not Monday to Friday.

## License

[MIT](LICENSE)
