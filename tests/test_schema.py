from datetime import UTC, date, datetime

import pyarrow as pa
import pytest

from pitdata.schema import RECORD_SCHEMA, SchemaError, validate

UTC_US = pa.timestamp("us", tz="UTC")


def make_table(**overrides) -> pa.Table:
    """A small valid table. Pass column=pa.array(...) to replace one column,
    or column=None to drop it."""
    cols = {
        "series_id": pa.array(["FF3_DAILY.MKT_RF", "FF3_DAILY.MKT_RF"]),
        "valid_date": pa.array([date(2024, 1, 2), date(2024, 1, 3)]),
        "value": pa.array([0.25, -0.11]),
        "knowledge_time": pa.array([datetime(2024, 2, 29, tzinfo=UTC)] * 2, type=UTC_US),
        "knowledge_basis": pa.array(["assumed", "assumed"]),
        "ingest_time": pa.array([datetime(2026, 11, 12, 15, 0, tzinfo=UTC)] * 2, type=UTC_US),
        "ingest_id": pa.array(["abc123", "abc123"]),
        "source": pa.array(["ken_french", "ken_french"]),
    }
    for name, arr in overrides.items():
        if arr is None:
            cols.pop(name)
        else:
            cols[name] = arr
    return pa.table(cols)


def test_valid_table_passes_and_matches_schema_exactly():
    result = validate(make_table())
    assert result.schema.equals(RECORD_SCHEMA, check_metadata=True)
    assert result.num_rows == 2


def test_columns_are_reordered_to_schema_order():
    table = make_table()
    shuffled = table.select(list(reversed(table.column_names)))
    assert validate(shuffled).column_names == RECORD_SCHEMA.names


def test_missing_column_is_rejected():
    with pytest.raises(SchemaError, match="missing=.*source"):
        validate(make_table(source=None))


def test_extra_column_is_rejected():
    table = make_table().append_column("note", pa.array(["x", "y"]))
    with pytest.raises(SchemaError, match="extra=.*note"):
        validate(table)


def test_naive_timestamp_is_rejected():
    # Deliberately naive: this is the input the schema must reject.
    naive = pa.array([datetime(2024, 2, 29)] * 2, type=pa.timestamp("us"))  # noqa: DTZ001
    with pytest.raises(SchemaError, match="knowledge_time"):
        validate(make_table(knowledge_time=naive))


def test_non_utc_timestamp_is_rejected():
    eastern = pa.array(
        [datetime(2024, 2, 29, tzinfo=UTC)] * 2,
        type=pa.timestamp("us", tz="America/New_York"),
    )
    with pytest.raises(SchemaError, match="ingest_time"):
        validate(make_table(ingest_time=eastern))


def test_nanosecond_timestamps_are_converted_to_microseconds():
    ns = pa.array([datetime(2024, 2, 29, tzinfo=UTC)] * 2, type=pa.timestamp("ns", tz="UTC"))
    result = validate(make_table(knowledge_time=ns))
    assert result.schema.field("knowledge_time").type == UTC_US


def test_wrong_value_type_is_rejected():
    with pytest.raises(SchemaError, match="value"):
        validate(make_table(value=pa.array(["0.25", "-0.11"])))


def test_null_value_is_allowed():
    result = validate(make_table(value=pa.array([0.25, None])))
    assert result.column("value").null_count == 1


def test_null_in_required_column_is_rejected():
    with pytest.raises(SchemaError, match="series_id"):
        validate(make_table(series_id=pa.array(["FF3_DAILY.MKT_RF", None])))


def test_unknown_knowledge_basis_is_rejected():
    with pytest.raises(SchemaError, match="knowledge_basis"):
        validate(make_table(knowledge_basis=pa.array(["assumed", "guessed"])))


def test_knowledge_time_before_valid_date_is_rejected():
    too_early = pa.array(
        [datetime(2024, 1, 2, tzinfo=UTC), datetime(2023, 12, 31, tzinfo=UTC)],
        type=UTC_US,
    )
    with pytest.raises(SchemaError, match="1 row"):
        validate(make_table(knowledge_time=too_early))


def test_knowledge_time_on_valid_date_is_allowed():
    same_day = pa.array(
        [
            datetime(2024, 1, 2, 21, 0, tzinfo=UTC),
            datetime(2024, 1, 3, tzinfo=UTC),
        ],
        type=UTC_US,
    )
    validate(make_table(knowledge_time=same_day))


def test_empty_table_passes():
    validate(make_table().slice(0, 0))
