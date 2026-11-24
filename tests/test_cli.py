import io
import zipfile
from datetime import date, timedelta
from functools import partial
from pathlib import Path

import pytest
import requests
from typer.testing import CliRunner

from pitdata import cli
from pitdata.sources.fama_french import SOURCE, ingest_ff3_daily

FIXTURE = Path(__file__).parent / "fixtures" / "ff3_daily_sample.csv"
runner = CliRunner()


def make_zip(csv: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("F-F_Research_Data_Factors_daily.csv", csv)
    return buf.getvalue()


def fixture_zip() -> bytes:
    return make_zip(FIXTURE.read_bytes())


@pytest.fixture
def use_fetch(monkeypatch):
    """Make the CLI's ingest use `fetch` instead of the network.

    Patching fama_french.download would not work: a default argument value is
    bound when the function is defined, so ingest_ff3_daily already holds the
    original download. Wrapping the function the CLI calls does work.
    """

    def install(fetch):
        monkeypatch.setattr(cli, "ingest_ff3_daily", partial(ingest_ff3_daily, fetch=fetch))

    return install


def run(root: Path):
    return runner.invoke(cli.app, ["ingest", "ff", "--root", str(root)])


def test_first_run_writes_second_run_is_unchanged(tmp_path, use_fetch):
    use_fetch(lambda url: fixture_zip())

    first = run(tmp_path)
    assert first.exit_code == 0, first.output
    assert first.output.startswith("written 28 rows (ingest ")

    second = run(tmp_path)
    assert second.exit_code == 0, second.output
    ingest_id = first.output.split("(ingest ")[1].rstrip(")\n")
    assert second.output == f"unchanged, matches ingest {ingest_id}\n"
    assert [p.name for p in (tmp_path / SOURCE).iterdir()] == [ingest_id]


def test_row_count_uses_thousands_separators(tmp_path, use_fetch):
    # 300 dates x 4 series = 1,200 rows.
    start = date(2020, 1, 1)
    rows = [f"{start + timedelta(days=i):%Y%m%d},  0.10,  0.20,  0.30,  0.01" for i in range(300)]
    csv = "\r\n".join(["Preamble.", "", ",Mkt-RF,SMB,HML,RF", *rows, "", "Copyright."]) + "\r\n"
    use_fetch(lambda url: make_zip(csv.encode()))
    result = run(tmp_path)
    assert result.exit_code == 0, result.output
    assert result.output.startswith("written 1,200 rows (ingest ")


def test_network_failure_exits_nonzero_with_the_error(tmp_path, use_fetch):
    def fail(url):
        raise requests.ConnectionError("connection refused")

    use_fetch(fail)
    result = run(tmp_path)
    assert result.exit_code == 1
    assert "error: connection refused" in result.output
    assert not (tmp_path / SOURCE).exists()


def test_parse_failure_exits_nonzero_with_the_error(tmp_path, use_fetch):
    use_fetch(lambda url: b"<html>not a zip</html>")
    result = run(tmp_path)
    assert result.exit_code == 1
    assert "error: download is not a zip archive" in result.output


# --- pit build -------------------------------------------------------------------


def run_build(raw: Path, out: Path, *extra: str):
    return runner.invoke(cli.app, ["build", "--raw", str(raw), "--out", str(out), *extra])


def test_build_prints_a_summary_and_one_warning_line_and_still_succeeds(tmp_path, use_fetch):
    # The fixture has two missing-value markers on purpose, so the null check
    # warns. Warnings never fail the build (D-023).
    use_fetch(lambda url: fixture_zip())
    raw, out = tmp_path / "raw", tmp_path / "validated"
    assert run(raw).exit_code == 0

    result = run_build(raw, out)
    assert result.exit_code == 0, result.output
    assert result.output == (
        f"ken_french: 28 records, 0 restated keys, from 1 ingest -> {out / 'ken_french.parquet'}\n"
        f"ken_french: quality warnings: nulls (2); see {out / 'ken_french.quality.json'}\n"
    )


def test_build_with_no_raw_data_says_so(tmp_path):
    result = run_build(tmp_path / "raw", tmp_path / "validated")
    assert result.exit_code == 0
    assert result.output.startswith("nothing to build")


def test_build_with_unregistered_raw_folder_exits_nonzero(tmp_path, use_fetch):
    use_fetch(lambda url: fixture_zip())
    raw = tmp_path / "raw"
    assert run(raw).exit_code == 0
    (raw / "fred").mkdir()

    result = run_build(raw, tmp_path / "validated")
    assert result.exit_code == 1
    assert "error: raw folders with no registered config: ['fred']" in result.output

    only = run_build(raw, tmp_path / "validated", "--source", "ken_french")
    assert only.exit_code == 0, only.output


# --- pit get ---------------------------------------------------------------------


def test_get_prints_rows_known_as_of(tmp_path, use_fetch):
    use_fetch(lambda url: fixture_zip())
    raw, out = tmp_path / "raw", tmp_path / "validated"
    assert run(raw).exit_code == 0
    assert run_build(raw, out).exit_code == 0

    result = runner.invoke(
        cli.app,
        [
            "get",
            "FF3_DAILY.MKT_RF",
            "--start",
            "2024-01-02",
            "--end",
            "2024-01-03",
            "--asof",
            "2024-05-01T00:00Z",
            "--root",
            str(out),
        ],
    )
    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert lines[0].split() == [
        "2024-01-02",
        "FF3_DAILY.MKT_RF",
        "0.0025",
        "2024-04-01T00:00:00Z",
        "assumed",
    ]
    assert lines[1].split()[2] == "-0.0107"
    assert lines[-1] == "2 rows known as of 2024-05-01T00:00Z"


@pytest.mark.parametrize(
    ("asof", "message"),
    [
        ("2024-05-01T00:00", "asof must be time-zone-aware UTC"),  # no zone
        ("2024-05-01", "asof must be time-zone-aware UTC"),  # a date, parsed as naive midnight
        ("2024-05-01T00:00+05:00", "asof must be time-zone-aware UTC"),
    ],
)
def test_get_rejects_asof_without_explicit_utc(tmp_path, asof, message):
    result = runner.invoke(
        cli.app,
        [
            "get",
            "X",
            "--start",
            "2024-01-02",
            "--end",
            "2024-01-03",
            "--asof",
            asof,
            "--root",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 1
    assert message in result.output


def test_get_without_build_says_so(tmp_path):
    result = runner.invoke(
        cli.app,
        [
            "get",
            "X",
            "--start",
            "2024-01-02",
            "--end",
            "2024-01-03",
            "--asof",
            "2024-05-01T00:00Z",
            "--root",
            str(tmp_path / "validated"),
        ],
    )
    assert result.exit_code == 1
    assert "run `pit build` first" in result.output
