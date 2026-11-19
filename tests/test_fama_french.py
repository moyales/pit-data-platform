from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from hypothesis import given
from hypothesis import strategies as st

from pitdata.sources.fama_french import SOURCE, ParseError, parse_ff3_daily

FIXTURE = Path(__file__).parent / "fixtures" / "ff3_daily_sample.csv"
SERIES = ["FF3_DAILY.MKT_RF", "FF3_DAILY.SMB", "FF3_DAILY.HML", "FF3_DAILY.RF"]
HEADER = ",Mkt-RF,SMB,HML,RF"


def fixture_text() -> str:
    # read_bytes().decode(), not read_text(): read_text translates CRLF to LF,
    # which would hide the line endings the real file uses.
    return FIXTURE.read_bytes().decode("ascii")


def make_text(rows, header=HEADER, before=("Preamble.", ""), after=("", "Copyright.")) -> str:
    """Build a small file in the real layout; variants override one part."""
    return "\n".join([*before, header, *rows, *after]) + "\n"


ROWS = [
    "20240102,    0.25,   -0.31,    0.12,    0.02",
    "20240103,   -1.07,    0.44,   -0.05,    0.02",
]


def values_on(table: pa.Table, day: date) -> dict[str, float | None]:
    rows = table.to_pylist()
    return {r["series_id"]: r["value"] for r in rows if r["valid_date"] == day}


# --- the fixture -------------------------------------------------------------


def test_fixture_keeps_crlf_line_endings():
    # Guards the .gitattributes rule: if git ever converts the fixture to LF,
    # the tests stop covering the real file's line endings.
    assert b"\r\n" in FIXTURE.read_bytes()


def test_fixture_row_count_is_dates_times_four():
    table = parse_ff3_daily(fixture_text())
    assert table.num_rows == 7 * 4


def test_columns_and_types():
    table = parse_ff3_daily(fixture_text())
    assert table.schema == pa.schema(
        [
            ("series_id", pa.string()),
            ("valid_date", pa.date32()),
            ("value", pa.float64()),
            ("source", pa.string()),
        ]
    )
    assert set(table.column("source").to_pylist()) == {SOURCE}


def test_long_format_is_grouped_by_series_then_date():
    table = parse_ff3_daily(fixture_text())
    ids = table.column("series_id").to_pylist()
    assert ids == [s for s in SERIES for _ in range(7)]
    first_series_dates = table.column("valid_date").to_pylist()[:7]
    assert first_series_dates[0] == date(2024, 1, 2)
    assert first_series_dates[-1] == date(2024, 1, 10)


def test_values_are_exact_and_stay_in_percent():
    table = parse_ff3_daily(fixture_text())
    assert values_on(table, date(2024, 1, 3)) == {
        "FF3_DAILY.MKT_RF": -1.07,
        "FF3_DAILY.SMB": 0.44,
        "FF3_DAILY.HML": -0.05,
        "FF3_DAILY.RF": 0.02,
    }
    # 10.42 percent stays 10.42, not 0.1042: conversion belongs to the validated layer.
    assert values_on(table, date(2024, 1, 9))["FF3_DAILY.MKT_RF"] == 10.42


def test_missing_codes_become_null_and_zero_stays_zero():
    table = parse_ff3_daily(fixture_text())
    assert values_on(table, date(2024, 1, 4))["FF3_DAILY.HML"] is None  # -99.99
    assert values_on(table, date(2024, 1, 9))["FF3_DAILY.RF"] is None  # -999
    assert values_on(table, date(2024, 1, 4))["FF3_DAILY.MKT_RF"] == 0.0
    assert table.column("value").null_count == 2


def test_lf_and_crlf_give_the_same_table():
    crlf = fixture_text()
    assert parse_ff3_daily(crlf).equals(parse_ff3_daily(crlf.replace("\r\n", "\n")))


# --- where the table starts and ends -----------------------------------------


def test_longer_preamble_does_not_matter():
    # The header is found by content, so a preamble that gains lines is fine.
    longer = make_text(ROWS, before=("One.", "Two.", "Three.", "Four.", ""))
    assert parse_ff3_daily(longer).equals(parse_ff3_daily(make_text(ROWS)))


def test_parsing_stops_at_the_first_blank_line():
    # Monthly files have a second (annual) table after a blank line; it must
    # never be read as more rows. Its rows ("2024,...") would raise if read.
    # The header here differs from HEADER on purpose: a repeated identical
    # header raises (see test_header_twice_raises), which is right for the
    # daily file but means a monthly parser needs its own header rule (D-014).
    second_table = (
        "",
        " Annual Factors: January-December ",
        "Year,Mkt-RF,SMB,HML,RF",
        "2024,  1.00,  1.00,  1.00,  1.00",
    )
    table = parse_ff3_daily(make_text(ROWS, after=second_table))
    assert table.num_rows == 2 * 4


def test_end_of_text_without_blank_line_is_accepted():
    assert parse_ff3_daily(make_text(ROWS, after=())).num_rows == 2 * 4


def test_copyright_line_without_blank_line_raises():
    # A non-data line ends nothing: it raises (D-014). This is the safe failure
    # when the layout changes.
    # Lines: 1 preamble, 2 blank, 3 header, 4-5 rows, 6 copyright.
    with pytest.raises(ParseError, match="line 6"):
        parse_ff3_daily(make_text(ROWS, after=("Copyright.",)))


def test_corrupt_row_in_the_middle_raises_instead_of_truncating():
    rows = [ROWS[0], "2024-01-03,  0.10,  0.10,  0.10,  0.10", "20240104,  0.1,  0.1,  0.1,  0.1"]
    with pytest.raises(ParseError, match="line 5: date '2024-01-03'"):
        parse_ff3_daily(make_text(rows))


# --- header ------------------------------------------------------------------


@pytest.mark.parametrize(
    "header",
    [",Mkt_RF,SMB,HML,RF", ",Mkt-RF,SMB,HML", ",Mkt-RF,SMB,HML,RF,CMA", "Date,Mkt-RF,SMB,HML,RF"],
)
def test_wrong_header_raises(header):
    with pytest.raises(ParseError, match="not found"):
        parse_ff3_daily(make_text(ROWS, header=header))


def test_padded_header_is_accepted():
    assert parse_ff3_daily(make_text(ROWS, header=" , Mkt-RF , SMB , HML , RF ")).num_rows == 8


def test_header_twice_raises():
    with pytest.raises(ParseError, match="more than once"):
        parse_ff3_daily(make_text(ROWS, after=("", HEADER)))


def test_header_without_rows_raises():
    with pytest.raises(ParseError, match="no data rows"):
        parse_ff3_daily(make_text([]))


# --- rows --------------------------------------------------------------------


def test_unsorted_dates_raise():
    with pytest.raises(ParseError, match="before"):
        parse_ff3_daily(make_text(list(reversed(ROWS))))


def test_duplicate_date_raises():
    with pytest.raises(ParseError, match="duplicate date"):
        parse_ff3_daily(make_text([ROWS[0], ROWS[0]]))


@pytest.mark.parametrize("bad", ["2024-01-03", "2024013", "202401031", "20240230", "2024O103", ""])
def test_bad_date_raises(bad):
    with pytest.raises(ParseError, match="line 5"):
        parse_ff3_daily(make_text([ROWS[0], f"{bad},  0.1,  0.1,  0.1,  0.1"]))


@pytest.mark.parametrize(
    "row", ["20240103,  0.1,  0.1,  0.1", "20240103,  0.1,  0.1,  0.1,  0.1,  0.1"]
)
def test_wrong_field_count_raises(row):
    with pytest.raises(ParseError, match="expected 5 fields"):
        parse_ff3_daily(make_text([ROWS[0], row]))


@pytest.mark.parametrize("bad", ["nan", "inf", "-inf", "1_000", "1e-3", "+0.5", ".5", "", "abc"])
def test_value_that_is_not_a_plain_decimal_raises(bad):
    # All but the last three would be accepted by float() (D-015).
    with pytest.raises(ParseError, match="not a number"):
        parse_ff3_daily(make_text([ROWS[0], f"20240103,  {bad},  0.1,  0.1,  0.1"]))


# --- property ----------------------------------------------------------------


def _render(hundredths: int) -> str:
    # Two-decimal percent text, as the file prints it: -5 -> "-0.05".
    return str(Decimal(hundredths).scaleb(-2))


@given(
    st.lists(
        st.tuples(
            st.integers(0, 40_000),  # day offset from 1926-07-01
            st.lists(st.integers(-5_000, 5_000), min_size=4, max_size=4),  # within +-50%
        ),
        min_size=1,
        max_size=30,
        unique_by=lambda r: r[0],
    )
)
def test_rendered_rows_parse_back_exactly(rows):
    """Any well-formed file parses back to exactly the numbers written into it."""
    rows = sorted(rows)
    start = date(1926, 7, 1)
    days = [start + timedelta(days=offset) for offset, _ in rows]
    lines = [
        f"{d:%Y%m%d}," + ",".join(f"{_render(v):>8}" for v in vals)
        for d, (_, vals) in zip(days, rows, strict=True)
    ]

    table = parse_ff3_daily(make_text(lines))

    assert table.num_rows == 4 * len(rows)
    expected = [float(_render(vals[i])) for i in range(4) for _, vals in rows]
    assert table.column("value").to_pylist() == expected
    assert table.column("valid_date").to_pylist() == days * 4
