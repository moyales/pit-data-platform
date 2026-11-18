"""The record shapes shared by every source.

Every observation, whether a factor return, a price or a macro value, is stored
as one row in a long-format schema. There are two:

- RAW_SCHEMA: what a source file said and when we fetched it. No knowledge time,
  because that is an interpretation derived later (see D-006).
- RECORD_SCHEMA: a raw row plus `knowledge_time` and `knowledge_basis`. Used by
  the validated layer and the query API.

Every writer must pass its table through `validate()` before anything reaches disk.
"""

import pyarrow as pa
import pyarrow.compute as pc

SCHEMA_VERSION = "1"

# Allowed values for `knowledge_basis`.
#   observed: knowledge_time was seen at (or near) publication, by us or the vendor.
#   assumed:  knowledge_time was set by a documented lag rule, e.g. for backfilled history.
KNOWLEDGE_BASES = frozenset({"observed", "assumed"})

# Microsecond precision is plenty for daily data and is what DuckDB uses natively.
_TS = pa.timestamp("us", tz="UTC")

# Each field is defined once; both schemas are built from these by name, so a
# change to a type or nullability cannot make the two schemas drift apart.
_FIELDS = {
    f.name: f
    for f in [
        pa.field("series_id", pa.string(), nullable=False),
        pa.field("valid_date", pa.date32(), nullable=False),
        # Nullable on purpose: a source's missing-value code becomes null, never zero.
        pa.field("value", pa.float64(), nullable=True),
        pa.field("knowledge_time", _TS, nullable=False),
        pa.field("knowledge_basis", pa.string(), nullable=False),
        pa.field("ingest_time", _TS, nullable=False),
        pa.field("ingest_id", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
    ]
}


def _schema(*names: str) -> pa.Schema:
    return pa.schema(
        [_FIELDS[n] for n in names],
        metadata={"pitdata_schema_version": SCHEMA_VERSION},
    )


RAW_SCHEMA = _schema(
    "series_id",
    "valid_date",
    "value",
    "ingest_time",
    "ingest_id",
    "source",
)

RECORD_SCHEMA = _schema(
    "series_id",
    "valid_date",
    "value",
    "knowledge_time",
    "knowledge_basis",
    "ingest_time",
    "ingest_id",
    "source",
)


class SchemaError(ValueError):
    """Raised when a table does not match the schema it is validated against."""


def validate(table: pa.Table, schema: pa.Schema = RECORD_SCHEMA) -> pa.Table:
    """Check `table` against `schema` and return it in canonical form.

    `schema` is RAW_SCHEMA for the raw writer and RECORD_SCHEMA (the default)
    for everything downstream. The default fails safe: a raw table validated
    without passing RAW_SCHEMA is rejected for missing knowledge columns.

    Canonical form means: columns in schema order, timestamps in microseconds,
    and the schema's nullability flags and version metadata attached.

    Fails loudly instead of coercing. The only conversion allowed is the
    timestamp unit (e.g. pandas' nanoseconds to microseconds), because that
    loses no meaning; a lossy conversion raises an error during the cast.
    """
    _check_columns(table, schema)
    _check_types(table, schema)
    _check_nulls(table, schema)
    # These two checks only make sense where knowledge time exists (not in raw).
    if "knowledge_basis" in schema.names:
        _check_knowledge_basis(table)
    if "knowledge_time" in schema.names:
        _check_knowledge_not_before_valid(table)
    return table.select(schema.names).cast(schema)


def _check_columns(table: pa.Table, schema: pa.Schema) -> None:
    # Checked first: the set comparison below cannot see duplicate names, and
    # pyarrow tables allow them.
    names = table.column_names
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise SchemaError(f"duplicate columns: {duplicates}")
    expected = set(schema.names)
    actual = set(table.column_names)
    missing = expected - actual
    extra = actual - expected
    if missing or extra:
        raise SchemaError(f"column mismatch: missing={sorted(missing)}, extra={sorted(extra)}")


def _check_types(table: pa.Table, schema: pa.Schema) -> None:
    for field in schema:
        actual = table.schema.field(field.name).type
        if pa.types.is_timestamp(field.type):
            # Naive timestamps are rejected outright: they are the classic source
            # of off-by-one-day lookahead when time zones get mixed.
            if not pa.types.is_timestamp(actual) or actual.tz != "UTC":
                raise SchemaError(
                    f"{field.name}: expected a UTC timestamp, got {actual}. "
                    "Convert to UTC explicitly before validating."
                )
        elif actual != field.type:
            raise SchemaError(f"{field.name}: expected {field.type}, got {actual}")


def _check_nulls(table: pa.Table, schema: pa.Schema) -> None:
    for field in schema:
        if not field.nullable and table.column(field.name).null_count > 0:
            raise SchemaError(f"{field.name}: contains nulls but is required")


def _check_knowledge_basis(table: pa.Table) -> None:
    found = set(table.column("knowledge_basis").unique().to_pylist())
    unknown = found - KNOWLEDGE_BASES
    if unknown:
        raise SchemaError(f"knowledge_basis: unknown values {sorted(unknown)}")


def _check_knowledge_not_before_valid(table: pa.Table) -> None:
    """A value cannot be known before the start of the period it describes.

    If this fires, the row's timestamps are wrong, and a wrong knowledge_time
    is exactly how lookahead bias gets into a backtest.
    """
    if table.num_rows == 0:
        return
    # safe=False truncates the time of day; the timestamps are UTC, so the
    # truncation gives the UTC calendar date.
    known_date = pc.cast(table.column("knowledge_time"), pa.date32(), safe=False)
    too_early = pc.less(known_date, table.column("valid_date"))
    n_bad = pc.sum(pc.cast(too_early, pa.int64())).as_py() or 0
    if n_bad:
        raise SchemaError(f"{n_bad} row(s) have knowledge_time before valid_date")
