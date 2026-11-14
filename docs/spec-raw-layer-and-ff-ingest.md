# Spec: raw layer and Fama-French ingest (Steps 8–9)

*Agreed Nov 13, 2026. Implements decisions D-006 to D-011 in `docs/design-notes.md`. This spec replaces Steps 8–9 of the original build guide wherever the two differ.*

## The key change from the original plan

Raw stores **only what was observed**: what the source file said, and when we fetched it. It does not store `knowledge_time` or `knowledge_basis`. Those are derived later, in the validated layer (Week 2), by replaying the raw ingests in order. Knowledge time is an interpretation that depends on ingest history and on a lag rule. Putting it in raw would bake the lag rule into files that must never be edited. See D-006.

## Task order

0. **Pre-flight.** Run `uv run pytest -v`, `uv run ruff check .` and `uv run ruff format --check .` and fix what fails before starting. Two suspected issues in `schema.py` as committed on `schema-build`:
   - `SCHEMA_VERSION = 1` is an int, but pyarrow schema metadata values must be `str` or `bytes`. Expect an error at import or when the schema is built. Use `"1"`.
   - Top-level definitions are separated by one blank line; `ruff format` wants two, so the CI format check will fail.
1. **Schema refactor** (below).
2. **`raw.py`** and `tests/test_raw.py`.
3. **`parse_ff3_daily`** and its fixture tests.
4. **`extract_csv`, `download`, `ingest_ff3_daily`.**
5. **CLI:** `pit ingest ff`.
6. **Docs:** new design-log entries if anything changed during implementation, and the README's Known limitations.

One pull request per task, or per pair of small tasks. Explain the approach before writing each component (see CLAUDE.md, "How to work with me").

## 1. Schema refactor (`schema.py`)

Two schemas, one validator.

- `RAW_SCHEMA`: `series_id`, `valid_date`, `value`, `ingest_time`, `ingest_id`, `source`. Same types, nullability and version metadata as today.
- `RECORD_SCHEMA`: unchanged (adds `knowledge_time`, `knowledge_basis`). Used by the validated layer and the query API.
- `validate(table, schema=RECORD_SCHEMA)`: the column, type and null checks are generic over `schema`. The `knowledge_basis` check and the knowledge-time-not-before-valid-date check run only when those columns are in `schema`.

Tests: the existing tests keep passing against `RECORD_SCHEMA`. Add: a valid raw table passes against `RAW_SCHEMA`; a raw table that includes `knowledge_time` is rejected as an extra column; a full record is rejected by `RAW_SCHEMA`.

## 2. Raw writer (`raw.py`)

```python
@dataclass(frozen=True)
class IngestResult:
    ingest_id: str
    status: Literal["written", "unchanged"]
    path: Path          # the ingest folder (the existing one when unchanged)
    row_count: int

def write_raw(
    root: Path,            # e.g. Path("data/raw")
    source: str,           # e.g. "ken_french"
    url: str,
    filename: str,         # original download name, e.g. "F-F_Research_Data_Factors_daily_CSV.zip"
    original: bytes,       # bytes exactly as downloaded
    content: bytes,        # the extracted payload the table was parsed from
    content_name: str,     # e.g. "F-F_Research_Data_Factors_daily.CSV"
    parsed: pa.Table,      # series_id, valid_date, value, source
    ingest_time: datetime, # injected, must be UTC-aware
) -> IngestResult: ...

def list_ingests(root: Path, source: str) -> list[dict]   # meta.json contents, sorted by ingest_time
def read_ingest(path: Path) -> pa.Table                    # parsed.parquet, validated against RAW_SCHEMA
```

Behaviour of `write_raw`, in order:

1. Reject `ingest_time` unless it is time-zone-aware UTC (`ValueError`).
2. `content_sha256 = sha256(content)`. Hash the **extracted content, not the download** (D-007): a zip stores timestamps of its members, so a rebuilt zip with identical numbers has different bytes.
3. If any `meta.json` under `root/source/` has the same `content_sha256`, return `status="unchanged"` with that ingest's id, path and row count. Write nothing.
4. `ingest_id = f"{ingest_time:%Y%m%dT%H%M%SZ}_{content_sha256[:12]}"`.
5. Add the `ingest_time` and `ingest_id` columns to `parsed`, then `validate(table, RAW_SCHEMA)`. The writer is the only place ingest metadata is set, and every write goes through validation (D-009).
6. Write into a temporary folder `root/source/.tmp-<ingest_id>-<random>/`:
   - `original/<filename>`: the download, byte for byte
   - `content/<content_name>`: the extracted payload
   - `parsed.parquet`
   - `meta.json`: `ingest_id`, `source`, `url`, `filename`, `content_name`, `original_sha256`, `content_sha256`, `row_count`, `ingest_time` (ISO 8601, UTC), `schema_version`, `pitdata_version`
7. `os.rename` the temp folder to `root/source/<ingest_id>/`. Raise if the target exists. The rename is what makes an ingest all-or-nothing (D-008). It is atomic only within one filesystem, which is why the temp folder lives inside `root/source/`; leave a comment saying so.
8. On any exception before the rename, remove the temp folder on a best-effort basis and re-raise. Everything that reads raw ignores folders whose names start with `.tmp-`.

There is **no shared manifest file**. The index of ingests is a scan of the `meta.json` files (`list_ingests`), or in DuckDB: `SELECT * FROM read_json('data/raw/*/*/meta.json')`. That removes the failure where data is written but the manifest append is not (D-008).

### Tests (`tests/test_raw.py`, all using `tmp_path` and fixed `ingest_time` values)

- Same content twice: one ingest folder; second call returns `unchanged` with the first ingest's id.
- Same content inside a different zip (different `original` bytes): `unchanged`.
- Different content: two folders, ids sort by time.
- A first ingest folder is byte-for-byte identical (hash every file) after a second, different ingest is written.
- `parsed` that fails validation: raises `SchemaError`; no final folder and no `.tmp-` folder remain.
- Naive or non-UTC `ingest_time`: raises `ValueError`.
- `meta.json` contains every listed field with the right values; `read_ingest` round-trips the table.
- `list_ingests` ignores `.tmp-` folders.

## 3–4. Fama-French ingest (`sources/fama_french.py`)

Keep input/output separate from logic, so the risky part is a pure function testable with fixtures (D-011).

```python
SOURCE = "ken_french"
FF3_DAILY_URL = "..."   # confirm the current link on the Kenneth French Data Library page

def download(url: str) -> bytes                   # requests, timeout, raise_for_status, project User-Agent; no retries
def extract_csv(zip_bytes: bytes) -> tuple[str, bytes]   # (member name, bytes); exactly one member, else raise
def parse_ff3_daily(text: str) -> pa.Table          # pure; returns series_id, valid_date, value, source
def ingest_ff3_daily(root: Path, ingest_time: datetime) -> IngestResult   # download -> extract -> parse -> write_raw
```

Parser rules:

- **Find the header by content, not by counting lines.** Locate the line whose fields are `""`, `Mkt-RF`, `SMB`, `HML`, `RF` (after stripping whitespace). If it is not found, or appears more than once, raise. Open the real file once and make the fixture match it exactly.
- **Read data rows until the first line that is not a data row** (blank line, copyright line). Never read past it; monthly files have a second, annual table after that point.
- **Dates:** exact format `%Y%m%d`; anything else raises.
- **Missing codes** (`-99.99`, `-999`) become null, never zero.
- **Values stay in percent.** Unit conversion belongs to the validated layer.
- **Checks before returning:** at least one row; dates strictly increasing; no duplicate dates; every data row has exactly five fields.
- **Long format:** each date becomes four rows, `series_id` in `FF3_DAILY.MKT_RF`, `FF3_DAILY.SMB`, `FF3_DAILY.HML`, `FF3_DAILY.RF`.

Fixtures in `tests/fixtures/` (hand-made, about 15 lines each, no vendor data beyond a few illustrative rows):

- `ff3_daily_sample.csv`: preamble, header, a few rows including one missing code, trailing copyright line.
- Variants built inside the tests where simpler: wrong header, unsorted dates, duplicate date, bad date, short row, second table after a blank line.

Tests: row count (rows x 4); dates and values exact; missing code is null; percent unchanged; each malformed variant raises; parsing stops at the boundary; `extract_csv` raises on a zip with zero or two members; `ingest_ff3_daily` with `download` monkeypatched to return a fixture zip writes once and is `unchanged` the second time.

`download` itself has no unit test. Verify it by running `uv run pit ingest ff` twice by hand: the first run writes, the second says unchanged. That is the end-to-end idempotency check.

## 5. CLI (`cli.py`)

`pit ingest ff [--root data/raw]` calls `ingest_ff3_daily(root, datetime.now(UTC))`. This is the **only** place the real clock is read (D-010). Print one line: `written 12,345 rows (ingest 20261113T205700Z_3fa1c9e2b7d4)` or `unchanged, matches ingest 20261113T205700Z_3fa1c9e2b7d4`.

## Not in this step (Week 2), recorded so the design is not lost

Knowledge-time derivation in the validated layer, by replaying ingests of a source in `ingest_time` order:

- For each `(series_id, valid_date)`, each distinct value gets one record. `knowledge_time` is the `ingest_time` of the first ingest in which that value appears, with `knowledge_basis = "observed"`.
- Rows present in the **first** ingest of a source instead get `knowledge_time` from a documented, conservative lag rule (for example, the last day of the month after `valid_date`), with `knowledge_basis = "assumed"`. Too late is safe; too early is lookahead.
- A restated value produces a new record with the later knowledge time. The older record stays, so as-of queries before the restatement still return the old value.
- This must be a pure, deterministic function of the raw ingests, so the validated layer can be rebuilt at any time and the lag rule can change without re-downloading.

## Acceptance criteria

- `uv run pytest -v`, `ruff check` and `ruff format --check` pass locally and in CI on the pull request.
- `uv run pit ingest ff` run twice: first `written`, then `unchanged`.
- An ingest folder holds exactly `original/`, `content/`, `parsed.parquet` and `meta.json`.
- `docs/design-notes.md` and the README's Known limitations reflect anything that changed during implementation.

## Limitations to carry into the README

- Observed knowledge time is only as precise as how often the ingest runs. Late is safe, but imprecise.
- Rows a source deletes in a later file are not recorded as withdrawn.
- One writer at a time; two concurrent ingests could both treat a hash as new.
- `ingest_time` trusts the machine's clock.
- Fama-French history before the first ingest is current-vintage data with an assumed lag, not true point-in-time.
