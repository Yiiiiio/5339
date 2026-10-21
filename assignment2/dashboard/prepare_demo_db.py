#!/usr/bin/env python3
"""Copy the A2 database and clear received prices for a fresh live replay.

The submitted database already holds a completed replay, so a map opened on it
shows final prices immediately. This copy keeps all A1 tables and C's schema
but starts with no price events. The source database is never modified.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from assignment2.storage.price_store import initialise_database


A2_ROOT = Path(__file__).resolve().parents[1]


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--source",
        type=Path,
        default=A2_ROOT / "data/final/project_a2.duckdb",
        help="A2 database to copy (left unchanged).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=A2_ROOT / "data/demo/demo_a2.duckdb",
        help="Demo database to create.",
    )
    parser.add_argument("--force", action="store_true", help="Replace an existing output database.")
    return parser.parse_args()


def prepare_demo_database(source: Path, output: Path, *, force: bool = False) -> int:
    """Create ``output`` without price events and return the removed count."""

    source = source.resolve()
    output = output.resolve()
    if not source.is_file():
        raise FileNotFoundError(f"Source database does not exist: {source}")
    if output == source:
        raise ValueError("--output must differ from --source")
    if output.exists() and not force:
        raise FileExistsError(f"{output} already exists; pass --force to replace it")

    output.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, output)
    initialise_database(output)
    # Drop and recreate rather than DELETE: deleting many indexed rows has
    # failed in DuckDB, and C's schema recreates the table and view.
    with duckdb.connect(str(output)) as connection:
        removed = connection.execute("SELECT count(*) FROM price_events").fetchone()[0]
        connection.execute("DROP VIEW IF EXISTS latest_station_price")
        connection.execute("DROP TABLE price_events")
    initialise_database(output)
    return removed


def main() -> int:
    args = parse_arguments()
    try:
        removed = prepare_demo_database(args.source, args.output, force=args.force)
    except (OSError, ValueError, duckdb.Error) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print(f"Created {args.output.resolve()} (removed {removed} copied price events)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
