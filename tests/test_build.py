"""pit build: derive the validated layer from real raw ingest folders.

Raw folders are created with ingest_ff3_daily and a fake download of the
sample CSV, so these tests exercise the same files the real CLI produces.
"""

import hashlib
import io
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from pitdata import build as build_mod
from pitdata.build import BuildError, build, build_source, read_build_info, read_validated
from pitdata.raw import list_ingests, read_ingest
from pitdata.schema import RECORD_SCHEMA
from pitdata.sources import CONFIGS
from pitdata.sources.fama_french import SOURCE, VALIDATION, ingest_ff3_daily
from pitdata.validated import derive_records

FIXTURE = Path(__file__).parent / "fixtures" / "ff3_daily_sample.csv"
T1 = datetime(2026, 11, 20, 21, 0, tzinfo=UTC)
T2 = T1 + timedelta(days=1)


def zipped(csv: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("F-F_Research_Data_Factors_daily.csv", csv)
    return buf.getvalue()


def ingest(raw: Path, csv: bytes, when: datetime) -> None:
    ingest_ff3_daily(raw, when, fetch=lambda url: zipped(csv))


def restated_csv() -> bytes:
    # One value restated: Mkt-RF on 2024-01-03 goes from -1.07 to -1.17.
    original = FIXTURE.read_bytes()
    assert original.count(b"-1.07") == 1
    return original.replace(b"-1.07", b"-1.17")


@pytest.fixture
def raw(tmp_path) -> Path:
    root = tmp_path / "raw"
    ingest(root, FIXTURE.read_bytes(), T1)
    return root


def file_hashes(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


# --- the file and what it contains -------------------------------------------


def test_build_writes_exactly_what_derive_records_returns(raw, tmp_path):
    out = tmp_path / "validated"
    (result,) = build(raw, out, CONFIGS)

    assert result.path == out / "ken_french.parquet"
    assert (result.record_count, result.restated_keys, result.ingest_count) == (28, 0, 1)
    ingests = [(m, read_ingest(raw / SOURCE / m["ingest_id"])) for m in list_ingests(raw, SOURCE)]
    assert read_validated(result.path).equals(derive_records(ingests, VALIDATION))
    assert read_validated(result.path).schema.equals(RECORD_SCHEMA, check_metadata=True)


def test_build_info_is_stored_inside_the_file(raw, tmp_path):
    (result,) = build(raw, tmp_path / "validated", CONFIGS)
    info = read_build_info(result.path)
    (ingest_id,) = [m["ingest_id"] for m in list_ingests(raw, SOURCE)]
    assert info == {
        "source": SOURCE,
        "lag_rule": {"name": "ff_month_end_plus_2", "version": "1", "months_after": 2},
        "divide_by": 100.0,
        "raw_ingest_ids": [ingest_id],
        "pitdata_version": build_mod.version("pitdata"),
        "record_count": 28,
        "restated_keys": 0,
    }


def test_build_info_is_readable_from_duckdb(raw, tmp_path):
    # The provenance travels with the file, so any Parquet tool can read it.
    (result,) = build(raw, tmp_path / "validated", CONFIGS)
    rows = duckdb.sql(
        f"SELECT decode(value) FROM parquet_kv_metadata('{result.path}') "
        "WHERE decode(key) = 'pitdata_build'"
    ).fetchall()
    assert '"ff_month_end_plus_2"' in rows[0][0]


def test_two_builds_give_equal_tables_and_identical_info(raw, tmp_path):
    first = build(raw, tmp_path / "a", CONFIGS)[0].path
    second = build(raw, tmp_path / "b", CONFIGS)[0].path
    assert read_validated(first).equals(read_validated(second))
    assert read_build_info(first) == read_build_info(second)


def test_restatement_is_counted_and_the_file_replaced(raw, tmp_path):
    out = tmp_path / "validated"
    build(raw, out, CONFIGS)
    ingest(raw, restated_csv(), T2)

    (result,) = build(raw, out, CONFIGS)
    assert (result.record_count, result.restated_keys, result.ingest_count) == (29, 1, 2)
    assert len(read_build_info(result.path)["raw_ingest_ids"]) == 2
    assert read_validated(result.path).num_rows == 29


def test_a_key_restated_twice_counts_once(raw, tmp_path):
    # Three records for one key is one restated key, not two. (Counting
    # "records minus keys" gives 2 here; with only one restatement both agree.)
    ingest(raw, restated_csv(), T2)
    ingest(raw, restated_csv().replace(b"-1.17", b"-1.27"), T2 + timedelta(days=1))
    (result,) = build(raw, tmp_path / "validated", CONFIGS)
    assert (result.record_count, result.restated_keys, result.ingest_count) == (30, 1, 3)


def test_values_are_decimals(raw, tmp_path):
    (result,) = build(raw, tmp_path / "validated", CONFIGS)
    table = read_validated(result.path)
    first_mkt = next(r for r in table.to_pylist() if r["series_id"] == "FF3_DAILY.MKT_RF")
    assert first_mkt["value"] == 0.0025  # 0.25 percent in the sample file


# --- safety --------------------------------------------------------------------


def test_build_never_changes_raw(raw, tmp_path):
    before = file_hashes(raw)
    build(raw, tmp_path / "validated", CONFIGS)
    assert file_hashes(raw) == before


def test_failure_before_the_swap_keeps_the_old_file_and_no_temp(raw, tmp_path, monkeypatch):
    out = tmp_path / "validated"
    (result,) = build(raw, out, CONFIGS)
    old_bytes = result.path.read_bytes()
    ingest(raw, restated_csv(), T2)

    def boom(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(build_mod.os, "replace", boom)
    with pytest.raises(OSError, match="disk full"):
        build(raw, out, CONFIGS)
    monkeypatch.undo()

    assert result.path.read_bytes() == old_bytes
    assert sorted(p.name for p in out.iterdir()) == ["ken_french.parquet"]


def test_stale_temp_files_are_ignored_and_left_alone(raw, tmp_path):
    out = tmp_path / "validated"
    out.mkdir()
    stale = out / ".tmp-ken_french-dead.parquet"
    stale.write_bytes(b"half a file")
    (result,) = build(raw, out, CONFIGS)
    assert read_validated(result.path).num_rows == 28
    assert stale.read_bytes() == b"half a file"


# --- which sources get built -----------------------------------------------------


def test_raw_folder_without_config_fails_and_writes_nothing(raw, tmp_path):
    (raw / "fred").mkdir()
    out = tmp_path / "validated"
    with pytest.raises(BuildError, match=r"no registered config: \['fred'\]"):
        build(raw, out, CONFIGS)
    assert not out.exists()


def test_only_builds_the_named_source(raw, tmp_path):
    (raw / "fred").mkdir()
    out = tmp_path / "validated"
    (result,) = build(raw, out, CONFIGS, only=SOURCE)
    assert result.source == SOURCE
    assert sorted(p.name for p in out.iterdir()) == ["ken_french.parquet"]


@pytest.mark.parametrize(
    ("only", "message"), [("fred", "unknown source"), ("ken_french", "no raw data")]
)
def test_only_rejects_unknown_or_missing_sources(tmp_path, only, message):
    raw = tmp_path / "raw"
    raw.mkdir()
    with pytest.raises(BuildError, match=message):
        build(raw, tmp_path / "validated", CONFIGS, only=only)


def test_hidden_entries_in_raw_are_not_sources(raw, tmp_path):
    (raw / ".DS_Store").write_bytes(b"")
    (raw / ".cache").mkdir()
    assert [r.source for r in build(raw, tmp_path / "validated", CONFIGS)] == [SOURCE]


def test_no_raw_folder_builds_nothing(tmp_path):
    assert build(tmp_path / "missing", tmp_path / "validated", CONFIGS) == []


def test_source_with_no_finished_ingests_writes_an_empty_valid_file(tmp_path):
    raw = tmp_path / "raw"
    (raw / SOURCE / ".tmp-crashed-abcd").mkdir(parents=True)
    result = build_source(raw, tmp_path / "validated", SOURCE, VALIDATION)
    assert result.record_count == 0
    assert read_validated(result.path).num_rows == 0
    assert read_build_info(result.path)["raw_ingest_ids"] == []


def test_read_build_info_rejects_a_file_not_written_by_build(tmp_path):
    import pyarrow.parquet as pq

    path = tmp_path / "other.parquet"
    pq.write_table(RECORD_SCHEMA.empty_table(), path)
    with pytest.raises(BuildError, match="no build information"):
        read_build_info(path)
