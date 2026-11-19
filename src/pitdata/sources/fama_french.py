"""Fama-French factors from the Kenneth French Data Library.

The parser is a pure function (text in, table out) so the risky logic is fully
testable with small fixtures and no network (D-011). Download and archive
handling stay at the edges and are kept deliberately thin:

    download(url) -> zip bytes -> extract_csv -> decode -> parse_ff3_daily -> write_raw
"""

import io
import re
import zipfile
import zlib
from collections.abc import Callable
from datetime import date, datetime
from importlib.metadata import version
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse

import pyarrow as pa
import requests

from pitdata.raw import IngestResult, write_raw
from pitdata.validated import LagRule, SourceConfig

SOURCE = "ken_french"

# How the validated layer treats this source (D-021, D-026):
# - Backfilled history is assumed known by the end of the second month after the
#   valid month. The one observed publication (2026-10-26, data through
#   2026-09-30) had about 4 weeks of lag; this allows about 9. Too late is safe.
# - Values are published in percent; the store holds decimals.
VALIDATION = SourceConfig(
    lag_rule=LagRule(name="ff_month_end_plus_2", version="1", months_after=2),
    divide_by=100.0,
)

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


# Size caps, enforced while reading rather than trusted from headers (D-018).
# The real zip is about 180 KB and the CSV about 1.2 MB, so these only trip on
# something wrong: a zip bomb, or a server sending something else entirely.
MAX_DOWNLOAD_BYTES = 20 * 1024 * 1024
MAX_CONTENT_BYTES = 50 * 1024 * 1024

# (connect, read) seconds. Without a timeout, requests can wait forever.
TIMEOUT = (10, 60)
# Generic on purpose: identifies the tool, never the person running it.
USER_AGENT = f"pitdata/{version('pitdata')} (personal research)"


class ParseError(ValueError):
    """Raised when a source file does not have the layout the parser expects."""


class DownloadError(ValueError):
    """Raised when a download is not what we can safely accept."""


class ExtractError(ValueError):
    """Raised when an archive does not hold exactly one readable file."""


def download(url: str, *, max_bytes: int = MAX_DOWNLOAD_BYTES) -> bytes:
    """Fetch `url` and return the body. No retries: a failure fails the ingest; re-run it."""
    with requests.get(
        url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT, stream=True
    ) as response:
        response.raise_for_status()
        # Streamed and counted, because Content-Length can be missing or wrong.
        chunks: list[bytes] = []
        total = 0
        for chunk in response.iter_content(chunk_size=64 * 1024):
            total += len(chunk)
            if total > max_bytes:
                raise DownloadError(f"{url}: response is larger than {max_bytes:,} bytes")
            chunks.append(chunk)
    return b"".join(chunks)


def extract_csv(zip_bytes: bytes, *, max_bytes: int = MAX_CONTENT_BYTES) -> tuple[str, bytes]:
    """Return (member name, bytes) of the zip's single member.

    The member name is returned as found; write_raw rejects any name that is
    not a plain file name, so a name like "../x.csv" cannot escape the folder.
    """
    try:
        archive = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile as e:
        raise ExtractError("download is not a zip archive") from e

    with archive:
        members = archive.infolist()
        if len(members) != 1:
            names = [m.filename for m in members]
            raise ExtractError(f"expected exactly one file in the zip, found {len(names)}: {names}")
        (member,) = members
        # Folder entries count as members, so "exactly one" alone would accept
        # a zip holding only an empty folder.
        if member.is_dir():
            raise ExtractError(
                f"expected exactly one file in the zip, found a folder: {member.filename}"
            )
        try:
            with archive.open(member) as f:
                # The size a zip declares can be faked, so read at most one byte
                # past the cap and check what actually came out (D-018).
                data = f.read(max_bytes + 1)
        except (zipfile.BadZipFile, zlib.error, EOFError) as e:
            # zipfile checks each member's CRC-32 when it reaches the end, so a
            # truncated or corrupted download fails here.
            raise ExtractError(f"{member.filename}: corrupt archive member: {e}") from e

    if len(data) > max_bytes:
        raise ExtractError(f"{member.filename}: larger than {max_bytes:,} bytes when extracted")
    return member.filename, data


def decode(content: bytes, name: str) -> str:
    """Decode as strict UTF-8 (D-017).

    The real file is plain ASCII, which UTF-8 reads unchanged. Strict means bytes
    that are not valid UTF-8 (say, a Windows-1252 file) raise instead of being
    guessed at. The parser's own checks protect the data rows.
    """
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError as e:
        raise ParseError(f"{name}: not valid UTF-8 ({e})") from e


def ingest_ff3_daily(
    root: Path,
    ingest_time: datetime,
    *,
    fetch: Callable[[str], bytes] = download,
) -> IngestResult:
    """Download, extract, parse and write one raw ingest of the daily three factors.

    `fetch` is injected, like the clock (D-010, D-019): tests pass a function
    returning fixture bytes, so this path is tested end to end without a network.
    """
    original = fetch(FF3_DAILY_URL)
    content_name, content = extract_csv(original)
    parsed = parse_ff3_daily(decode(content, content_name))
    return write_raw(
        root=root,
        source=SOURCE,
        url=FF3_DAILY_URL,
        filename=PurePosixPath(urlparse(FF3_DAILY_URL).path).name,
        original=original,
        content=content,
        content_name=content_name,
        parsed=parsed,
        ingest_time=ingest_time,
        parser_version=PARSER_VERSION,
    )


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
