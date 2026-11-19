"""Fama-French factors from the Kenneth French Data Library.

The parser is a pure function (text in, table out) so the risky logic is fully
testable with small fixtures and no network (D-011). Download and archive
handling stay at the edges.
"""

import re
from datetime import date

import pyarrow as pa

SOURCE = "ken_french"

# Confirmed on the data library page on 2026-11-18.
FF3_DAILY_URL = (
    "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"
    "F-F_Research_Data_Factors_daily_CSV.zip"
)

# Bump whenever a change to the parser could change its output (D-012).
PARSER_VERSION = "1"

# Header fields after stripping whitespace. The first column (the date) has no name.
_FF3_HEADER = ("", "Mkt-RF", "SMB", "HML", "RF")
# One series per value column, in header order.
_FF3_SERIES = ("FF3_DAILY.MKT_RF", "FF3_DAILY.SMB", "FF3_DAILY.HML", "FF3_DAILY.RF")

# The library's codes for a missing value. They become null, never zero. No real
# daily factor return is near -99.99%, so a genuine value cannot be mistaken for one.
_MISSING_CODES = frozenset({-99.99, -999.0})

# Exactly what the file uses: optional minus, digits, optional decimal part.
# float() alone is too lenient: it accepts "nan", "inf", "1e3" and "1_000" (D-015).
_NUMBER = re.compile(r"-?\d+(\.\d+)?")
_DATE = re.compile(r"\d{8}")


class ParseError(ValueError):
    """Raised when a source file does not have the layout the parser expects."""


def parse_ff3_daily(text: str) -> pa.Table:
    """Parse the daily three-factor CSV into long format.

    Returns columns `series_id`, `valid_date`, `value`, `source`: four rows per
    date. Values stay in percent; unit conversion belongs to the validated layer.

    The table is the lines between the header and the first blank line (or the
    end of the text). Every one of those lines must be a valid data row; a line
    that is not raises instead of quietly ending the table (D-014). Anything
    after the blank line, such as the copyright notice, is ignored.
    """
    lines = text.splitlines()  # handles the file's CRLF line endings as well as LF
    header_idx = _find_header(lines)

    dates: list[date] = []
    columns: list[list[float | None]] = [[] for _ in _FF3_SERIES]
    for idx in range(header_idx + 1, len(lines)):
        line = lines[idx]
        if not line.strip():
            break
        row_date, values = _parse_row(line, line_no=idx + 1)
        if dates and row_date == dates[-1]:
            raise ParseError(f"line {idx + 1}: duplicate date {row_date}")
        if dates and row_date < dates[-1]:
            raise ParseError(f"line {idx + 1}: date {row_date} is before {dates[-1]}")
        dates.append(row_date)
        for column, value in zip(columns, values, strict=True):
            column.append(value)

    if not dates:
        raise ParseError(f"no data rows after the header on line {header_idx + 1}")

    # Long format, grouped by series then date: deterministic, and each series is
    # contiguous, which compresses better in Parquet.
    n = len(dates)
    return pa.table(
        {
            "series_id": pa.array([s for s in _FF3_SERIES for _ in range(n)], type=pa.string()),
            "valid_date": pa.array(dates * len(_FF3_SERIES), type=pa.date32()),
            "value": pa.array([v for column in columns for v in column], type=pa.float64()),
            "source": pa.array([SOURCE] * (n * len(_FF3_SERIES)), type=pa.string()),
        }
    )


def _find_header(lines: list[str]) -> int:
    """Find the header by its content, not by counting lines (D-011).

    Counting lines breaks silently the day the preamble gains a sentence.
    """
    matches = [
        i
        for i, line in enumerate(lines)
        if tuple(f.strip() for f in line.split(",")) == _FF3_HEADER
    ]
    if not matches:
        raise ParseError(f"header {list(_FF3_HEADER)} not found")
    if len(matches) > 1:
        raise ParseError(f"header found more than once, on lines {[i + 1 for i in matches]}")
    return matches[0]


def _parse_row(line: str, line_no: int) -> tuple[date, list[float | None]]:
    fields = [f.strip() for f in line.split(",")]
    if len(fields) != len(_FF3_HEADER):
        raise ParseError(f"line {line_no}: expected {len(_FF3_HEADER)} fields, got {len(fields)}")
    return _parse_date(fields[0], line_no), [_parse_value(f, line_no) for f in fields[1:]]


def _parse_date(field: str, line_no: int) -> date:
    # A regex plus date() instead of strptime: strptime("%Y%m%d") accepts
    # unpadded input such as "2024112", which is ambiguous (Jan 12 or Nov 2?).
    if not _DATE.fullmatch(field):
        raise ParseError(f"line {line_no}: date {field!r} is not YYYYMMDD")
    try:
        return date(int(field[:4]), int(field[4:6]), int(field[6:]))
    except ValueError as e:  # e.g. 20240230
        raise ParseError(f"line {line_no}: {field!r} is not a real date") from e


def _parse_value(field: str, line_no: int) -> float | None:
    if not _NUMBER.fullmatch(field):
        raise ParseError(f"line {line_no}: {field!r} is not a number")
    value = float(field)
    return None if value in _MISSING_CODES else value
