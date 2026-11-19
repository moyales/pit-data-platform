"""Command-line interface: `pit ingest ff`.

This is the only place that reads the real clock (D-010). Everything below it
takes the time as an argument.
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import requests
import typer

from pitdata.sources.fama_french import ingest_ff3_daily

DEFAULT_ROOT = Path("data/raw")

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
