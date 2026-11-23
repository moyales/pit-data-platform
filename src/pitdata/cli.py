"""Command-line interface: `pit ingest ff` and `pit build`.

This is the only place that reads the real clock (D-010). Everything below it
takes the time as an argument.
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import requests
import typer

from pitdata import build as build_module
from pitdata.sources import CONFIGS
from pitdata.sources.fama_french import ingest_ff3_daily

DEFAULT_ROOT = Path("data/raw")
DEFAULT_VALIDATED = Path("data/validated")

app = typer.Typer(help="pitdata: a point-in-time research data store.", no_args_is_help=True)
ingest_app = typer.Typer(help="Download a source and write a raw ingest.", no_args_is_help=True)
app.add_typer(ingest_app, name="ingest")


@ingest_app.command("ff")
def ingest_ff(
    root: Annotated[Path, typer.Option(help="Folder holding the raw layer.")] = DEFAULT_ROOT,
) -> None:
    """Ingest the Fama-French daily three factors."""
    try:
        result = ingest_ff3_daily(root, datetime.now(UTC))
    # Expected failures: network errors, and every parse, schema, extract and
    # size error (all ValueError subclasses), and file system errors. Anything
    # else is a bug and should show its full traceback.
    except (requests.RequestException, ValueError, OSError) as e:
        typer.echo(f"error: {e}", err=True)
        raise typer.Exit(code=1) from e

    if result.status == "written":
        typer.echo(f"written {result.row_count:,} rows (ingest {result.ingest_id})")
    else:
        typer.echo(f"unchanged, matches ingest {result.ingest_id}")


@app.command("build")
def build(
    raw: Annotated[Path, typer.Option(help="Folder holding the raw layer.")] = DEFAULT_ROOT,
    out: Annotated[Path, typer.Option(help="Folder for the validated layer.")] = DEFAULT_VALIDATED,
    source: Annotated[
        str | None, typer.Option(help="Build only this source, e.g. ken_french.")
    ] = None,
) -> None:
    """Rebuild the validated layer from raw. Raw is only read."""
    try:
        results = build_module.build(raw, out, CONFIGS, only=source)
    # Expected failures: unknown sources, schema and input errors (ValueError
    # subclasses), and file system errors. Anything else is a bug.
    except (ValueError, OSError) as e:
        typer.echo(f"error: {e}", err=True)
        raise typer.Exit(code=1) from e

    if not results:
        typer.echo(f"nothing to build: no source folders under {raw}")
    for r in results:
        typer.echo(
            f"{r.source}: {r.record_count:,} records, {r.restated_keys:,} restated keys, "
            f"from {r.ingest_count:,} ingest{'s' if r.ingest_count != 1 else ''} -> {r.path}"
        )
