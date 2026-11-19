"""download, extract_csv, decode and ingest_ff3_daily. No test touches the network."""

import io
import json
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import requests

from pitdata.raw import list_ingests, read_ingest
from pitdata.sources import fama_french
from pitdata.sources.fama_french import (
    FF3_DAILY_URL,
    SOURCE,
    USER_AGENT,
    DownloadError,
    ExtractError,
    ParseError,
    decode,
    download,
    extract_csv,
    ingest_ff3_daily,
    parse_ff3_daily,
)

FIXTURE = Path(__file__).parent / "fixtures" / "ff3_daily_sample.csv"
CSV_NAME = "F-F_Research_Data_Factors_daily.csv"  # the real member name, lowercase .csv
T1 = datetime(2026, 11, 18, 21, 0, tzinfo=UTC)
T2 = T1 + timedelta(days=1)


def make_zip(
    members: dict[str, bytes], year: int = 2026, compression=zipfile.ZIP_DEFLATED
) -> bytes:
    """Build a zip in memory; a different `year` gives different bytes for the same content."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=compression) as zf:
        for name, data in members.items():
            zf.writestr(zipfile.ZipInfo(name, date_time=(year, 1, 1, 0, 0, 0)), data)
    return buf.getvalue()


def fixture_zip(year: int = 2026) -> bytes:
    return make_zip({CSV_NAME: FIXTURE.read_bytes()}, year=year)


class FakeFetch:
    """Stands in for download(): returns fixed bytes and records the URLs asked for."""

    def __init__(self, data: bytes):
        self.data = data
        self.urls: list[str] = []

    def __call__(self, url: str) -> bytes:
        self.urls.append(url)
        return self.data


# --- extract_csv -------------------------------------------------------------


def test_extract_returns_the_single_member():
    name, data = extract_csv(fixture_zip())
    assert name == CSV_NAME
    assert data == FIXTURE.read_bytes()


@pytest.mark.parametrize(
    "members",
    [{}, {"a.csv": b"x", "b.csv": b"y"}, {"notes/": b""}, {"a.csv": b"x", "notes/": b""}],
    ids=["empty", "two-files", "only-a-folder", "file-and-folder"],
)
def test_extract_requires_exactly_one_file(members):
    with pytest.raises(ExtractError, match="exactly one"):
        extract_csv(make_zip(members))


def test_extract_rejects_bytes_that_are_not_a_zip():
    # e.g. an HTML error page served with status 200.
    with pytest.raises(ExtractError, match="not a zip"):
        extract_csv(b"<html>Service unavailable</html>")


def test_extract_rejects_a_corrupted_member():
    # Stored uncompressed, so one byte of the CSV can be changed in place. The
    # CRC-32 recorded in the zip no longer matches.
    data = bytearray(make_zip({CSV_NAME: FIXTURE.read_bytes()}, compression=zipfile.ZIP_STORED))
    i = data.find(b"20240103")
    data[i] = ord("3")
    with pytest.raises(ExtractError, match="corrupt"):
        extract_csv(bytes(data))


def test_extract_enforces_the_size_cap_on_what_comes_out():
    content = FIXTURE.read_bytes()
    assert extract_csv(fixture_zip(), max_bytes=len(content))[1] == content
    with pytest.raises(ExtractError, match="larger than"):
        extract_csv(fixture_zip(), max_bytes=len(content) - 1)


def test_unsafe_member_name_is_rejected_by_the_writer(tmp_path):
    # extract_csv returns names as found; write_raw is the gate.
    fetch = FakeFetch(make_zip({"../evil.csv": FIXTURE.read_bytes()}))
    with pytest.raises(ValueError, match="plain file name"):
        ingest_ff3_daily(tmp_path, T1, fetch=fetch)
    assert not (tmp_path / SOURCE).exists()


# --- decode ------------------------------------------------------------------


def test_decode_accepts_ascii_and_utf8():
    assert decode("Copyright © 2026".encode(), "x.csv") == "Copyright © 2026"


def test_decode_rejects_bytes_that_are_not_utf8():
    # 0xA9 is "©" in Windows-1252, but not valid UTF-8 on its own.
    with pytest.raises(ParseError, match="x.csv: not valid UTF-8"):
        decode(b"Copyright \xa9 2026", "x.csv")


# --- download (with a fake response; no network) -----------------------------


class FakeResponse:
    def __init__(self, chunks: list[bytes], status: int = 200):
        self.chunks = chunks
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status >= 400:
            raise requests.HTTPError(f"{self.status} error")

    def iter_content(self, chunk_size):
        yield from self.chunks


@pytest.fixture
def fake_get(monkeypatch):
    """Replace requests.get; returns a function that sets the next response and records calls."""
    calls = []
    state = {}

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return state["response"]

    monkeypatch.setattr(fama_french.requests, "get", get)

    def respond(response):
        state["response"] = response
        return calls

    return respond


def test_download_returns_the_body_and_sends_timeout_and_user_agent(fake_get):
    calls = fake_get(FakeResponse([b"abc", b"def"]))
    assert download("https://example.com/f.zip") == b"abcdef"
    ((url, kwargs),) = calls
    assert url == "https://example.com/f.zip"
    assert kwargs["timeout"] == fama_french.TIMEOUT
    assert kwargs["headers"] == {"User-Agent": USER_AGENT}
    assert kwargs["stream"] is True


def test_user_agent_is_generic():
    assert USER_AGENT.startswith("pitdata/")
    assert "@" not in USER_AGENT


def test_download_raises_on_http_error(fake_get):
    fake_get(FakeResponse([b"Not found"], status=404))
    with pytest.raises(requests.HTTPError, match="404"):
        download("https://example.com/f.zip")


def test_download_enforces_the_size_cap_while_streaming(fake_get):
    fake_get(FakeResponse([b"x" * 6, b"x" * 6]))
    assert download("https://example.com/f.zip", max_bytes=12) == b"x" * 12
    with pytest.raises(DownloadError, match="larger than 11"):
        download("https://example.com/f.zip", max_bytes=11)


# --- ingest_ff3_daily --------------------------------------------------------


def test_ingest_writes_once_then_is_unchanged(tmp_path):
    fetch = FakeFetch(fixture_zip())
    first = ingest_ff3_daily(tmp_path, T1, fetch=fetch)
    second = ingest_ff3_daily(tmp_path, T2, fetch=fetch)

    assert fetch.urls == [FF3_DAILY_URL, FF3_DAILY_URL]
    assert first.status == "written"
    assert first.row_count == 7 * 4
    assert second.status == "unchanged"
    assert second.ingest_id == first.ingest_id
    assert len(list_ingests(tmp_path, SOURCE)) == 1


def test_same_csv_in_a_rebuilt_zip_is_unchanged(tmp_path):
    # The idempotency rule end to end (D-007): different zip bytes, same CSV.
    first = ingest_ff3_daily(tmp_path, T1, fetch=FakeFetch(fixture_zip(year=2025)))
    second = ingest_ff3_daily(tmp_path, T2, fetch=FakeFetch(fixture_zip(year=2026)))
    assert second.status == "unchanged"
    assert second.ingest_id == first.ingest_id


def test_ingest_stores_the_download_and_content_byte_for_byte(tmp_path):
    original = fixture_zip()
    result = ingest_ff3_daily(tmp_path, T1, fetch=FakeFetch(original))
    assert (
        result.path / "original" / "F-F_Research_Data_Factors_daily_CSV.zip"
    ).read_bytes() == original
    assert (result.path / "content" / CSV_NAME).read_bytes() == FIXTURE.read_bytes()


def test_ingest_meta_records_url_names_and_parser_version(tmp_path):
    result = ingest_ff3_daily(tmp_path, T1, fetch=FakeFetch(fixture_zip()))
    meta = json.loads((result.path / "meta.json").read_text())
    assert meta["url"] == FF3_DAILY_URL
    assert meta["filename"] == "F-F_Research_Data_Factors_daily_CSV.zip"
    assert meta["content_name"] == CSV_NAME
    assert meta["parser_version"] == "1"
    assert meta["source"] == SOURCE


def test_stored_table_is_the_parser_output_plus_ingest_columns(tmp_path):
    result = ingest_ff3_daily(tmp_path, T1, fetch=FakeFetch(fixture_zip()))
    stored = read_ingest(result.path)
    parsed = parse_ff3_daily(FIXTURE.read_bytes().decode())
    # Compared as rows: the stored schema has stricter nullability flags.
    assert stored.select(parsed.column_names).to_pylist() == parsed.to_pylist()
    assert set(stored.column("ingest_id").to_pylist()) == {result.ingest_id}


def test_unparseable_download_writes_nothing(tmp_path):
    bad_csv = FIXTURE.read_bytes().replace(b",Mkt-RF,", b",Mkt_RF,")
    with pytest.raises(ParseError, match="header"):
        ingest_ff3_daily(tmp_path, T1, fetch=FakeFetch(make_zip({CSV_NAME: bad_csv})))
    assert not (tmp_path / SOURCE).exists()


def test_non_utf8_download_writes_nothing(tmp_path):
    cp1252 = FIXTURE.read_bytes().replace(b"Copyright", b"Copyright \xa9")
    with pytest.raises(ParseError, match="not valid UTF-8"):
        ingest_ff3_daily(tmp_path, T1, fetch=FakeFetch(make_zip({CSV_NAME: cp1252})))
    assert not (tmp_path / SOURCE).exists()
