import hashlib
import io
import json
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pitdata import raw
from pitdata.raw import list_ingests, read_ingest, write_raw
from pitdata.schema import RAW_SCHEMA, SchemaError

T1 = datetime(2026, 11, 13, 20, 57, tzinfo=UTC)
T2 = T1 + timedelta(days=1)
SOURCE = "ken_french"
CONTENT = b"date,Mkt-RF\n20240102,0.25\n20240103,-0.11\n"
CONTENT_NAME = "sample.CSV"


def make_parsed(values=(0.25, -0.11), source=SOURCE) -> pa.Table:
    return pa.table(
        {
            "series_id": pa.array(["FF3_DAILY.MKT_RF"] * 2),
            "valid_date": pa.array([date(2024, 1, 2), date(2024, 1, 3)]),
            "value": pa.array(list(values), type=pa.float64()),
            "source": pa.array([source] * 2),
        }
    )


def make_zip(content: bytes, year: int = 2026) -> bytes:
    """Zip `content`; a different `year` gives different bytes for the same content."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(zipfile.ZipInfo(CONTENT_NAME, date_time=(year, 1, 1, 0, 0, 0)), content)
    return buf.getvalue()


def write(root: Path, content=CONTENT, ingest_time=T1, parsed=None, original=None, **kw):
    args = {
        "root": root,
        "source": SOURCE,
        "url": "https://example.com/sample.zip",
        "filename": "sample.zip",
        "original": make_zip(content) if original is None else original,
        "content": content,
        "content_name": CONTENT_NAME,
        "parsed": make_parsed() if parsed is None else parsed,
        "ingest_time": ingest_time,
        "parser_version": "1",
    }
    args.update(kw)
    return write_raw(**args)


def folders(root: Path) -> list[str]:
    return sorted(p.name for p in (root / SOURCE).iterdir())


def file_hashes(folder: Path) -> dict[str, str]:
    return {
        str(p.relative_to(folder)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(folder.rglob("*"))
        if p.is_file()
    }


def test_ingest_folder_holds_exactly_the_expected_files(tmp_path):
    result = write(tmp_path)
    assert result.status == "written"
    assert result.row_count == 2
    assert sorted(file_hashes(result.path)) == [
        "content/sample.CSV",
        "meta.json",
        "original/sample.zip",
        "parsed.parquet",
    ]
    assert (result.path / "content" / CONTENT_NAME).read_bytes() == CONTENT


def test_same_content_twice_is_unchanged(tmp_path):
    first = write(tmp_path, ingest_time=T1)
    second = write(tmp_path, ingest_time=T2)
    assert second.status == "unchanged"
    assert second.ingest_id == first.ingest_id
    assert second.path == first.path
    assert second.row_count == first.row_count
    assert folders(tmp_path) == [first.ingest_id]


def test_same_content_in_a_different_zip_is_unchanged(tmp_path):
    first = write(tmp_path, original=make_zip(CONTENT, year=2025))
    second = write(tmp_path, original=make_zip(CONTENT, year=2026), ingest_time=T2)
    assert make_zip(CONTENT, 2025) != make_zip(CONTENT, 2026)
    assert second.status == "unchanged"
    assert second.ingest_id == first.ingest_id


def test_different_content_writes_a_second_folder_and_ids_sort_by_time(tmp_path):
    first = write(tmp_path, ingest_time=T1)
    second = write(
        tmp_path,
        content=CONTENT + b"20240104,0.5\n",
        parsed=make_parsed((0.25, -0.12)),
        ingest_time=T2,
    )
    assert second.status == "written"
    assert folders(tmp_path) == [first.ingest_id, second.ingest_id]
    assert [m["ingest_id"] for m in list_ingests(tmp_path, SOURCE)] == folders(tmp_path)


def test_first_ingest_is_byte_identical_after_a_second_ingest(tmp_path):
    first = write(tmp_path, ingest_time=T1)
    before = file_hashes(first.path)
    write(tmp_path, content=CONTENT + b"x\n", ingest_time=T2)
    assert file_hashes(first.path) == before


def test_invalid_parsed_table_writes_nothing(tmp_path):
    bad = make_parsed().set_column(2, "value", pa.array(["0.25", "-0.11"]))
    with pytest.raises(SchemaError, match="value"):
        write(tmp_path, parsed=bad)
    assert not (tmp_path / SOURCE).exists() or folders(tmp_path) == []


def test_failure_after_writing_starts_removes_the_temp_folder(tmp_path, monkeypatch):
    # Fails after original/ and content/ are already on disk, so the cleanup
    # path really has something to remove.
    def boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(raw, "_parquet_bytes", boom)
    with pytest.raises(OSError, match="disk full"):
        write(tmp_path)
    assert folders(tmp_path) == []


def test_existing_target_folder_is_not_overwritten(tmp_path, monkeypatch):
    # Simulates a second writer creating the target after our unchanged scan.
    # The folder is empty, which os.rename alone would silently replace.
    sha = hashlib.sha256(CONTENT).hexdigest()
    target = tmp_path / SOURCE / f"{T1:%Y%m%dT%H%M%SZ}_{sha[:12]}"
    target.mkdir(parents=True)
    monkeypatch.setattr(raw, "list_ingests", lambda root, source: [])
    with pytest.raises(FileExistsError):
        write(tmp_path)
    assert folders(tmp_path) == [target.name]
    assert list(target.iterdir()) == []


@pytest.mark.parametrize(
    "bad_time",
    [
        datetime(2026, 11, 13, 20, 57),  # noqa: DTZ001 - naive on purpose
        datetime(2026, 11, 13, 15, 57, tzinfo=ZoneInfo("America/New_York")),
        # Offset 0 in winter, but not UTC.
        datetime(2026, 1, 13, 20, 57, tzinfo=ZoneInfo("Europe/London")),
    ],
)
def test_non_utc_ingest_time_is_rejected(tmp_path, bad_time):
    with pytest.raises(ValueError, match="UTC"):
        write(tmp_path, ingest_time=bad_time)


def test_zoneinfo_utc_is_accepted(tmp_path):
    assert write(tmp_path, ingest_time=T1.replace(tzinfo=ZoneInfo("UTC"))).status == "written"


@pytest.mark.parametrize("name", ["", ".", "..", "../x.csv", "a/b.csv", "a\\b.csv", "a\0b"])
@pytest.mark.parametrize("field", ["filename", "content_name"])
def test_unsafe_names_are_rejected(tmp_path, field, name):
    with pytest.raises(ValueError, match=field):
        write(tmp_path, **{field: name})
    assert not (tmp_path / SOURCE).exists()


def test_parsed_with_ingest_columns_is_rejected(tmp_path):
    parsed = make_parsed().append_column("ingest_id", pa.array(["x", "x"]))
    with pytest.raises(SchemaError, match="exactly"):
        write(tmp_path, parsed=parsed)


def test_parsed_with_duplicate_column_is_rejected(tmp_path):
    parsed = make_parsed().drop_columns(["value"]).append_column("source", pa.array(["a", "a"]))
    with pytest.raises(SchemaError, match="exactly"):
        write(tmp_path, parsed=parsed)


def test_parsed_with_another_source_is_rejected(tmp_path):
    with pytest.raises(SchemaError, match="source"):
        write(tmp_path, parsed=make_parsed(source="fred"))


def test_empty_parser_version_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="parser_version"):
        write(tmp_path, parser_version="")


def test_meta_json_has_every_field(tmp_path):
    original = make_zip(CONTENT)
    result = write(tmp_path, original=original)
    meta = json.loads((result.path / "meta.json").read_text())
    sha = hashlib.sha256(CONTENT).hexdigest()
    assert meta == {
        "ingest_id": f"20261113T205700Z_{sha[:12]}",
        "source": SOURCE,
        "url": "https://example.com/sample.zip",
        "filename": "sample.zip",
        "content_name": CONTENT_NAME,
        "original_sha256": hashlib.sha256(original).hexdigest(),
        "content_sha256": sha,
        "row_count": 2,
        "ingest_time": "2026-11-13T20:57:00+00:00",
        "schema_version": "1",
        "pitdata_version": raw.version("pitdata"),
        "parser_version": "1",
    }
    assert result.ingest_id == meta["ingest_id"]


def test_read_ingest_round_trips_the_table(tmp_path):
    result = write(tmp_path, parsed=make_parsed((0.25, None)))
    table = read_ingest(result.path)
    assert table.schema.equals(RAW_SCHEMA, check_metadata=True)
    assert table.column("value").to_pylist() == [0.25, None]
    assert table.column("ingest_id").to_pylist() == [result.ingest_id] * 2
    assert table.column("ingest_time").to_pylist() == [T1] * 2
    assert table.column("source").to_pylist() == [SOURCE] * 2


def test_parsed_parquet_is_stored_with_raw_schema(tmp_path):
    result = write(tmp_path)
    stored = pq.read_schema(result.path / "parsed.parquet")
    assert stored.names == RAW_SCHEMA.names


def test_list_ingests_ignores_temp_folders(tmp_path):
    result = write(tmp_path)
    (tmp_path / SOURCE / ".tmp-crashed-abcd").mkdir()
    assert [m["ingest_id"] for m in list_ingests(tmp_path, SOURCE)] == [result.ingest_id]


def test_list_ingests_of_unknown_source_is_empty(tmp_path):
    assert list_ingests(tmp_path, "nobody") == []
