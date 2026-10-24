#!/usr/bin/env python3
"""Prepare deduplicated MQTT price_update events from hourly price CSV data."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path


REQUIRED_COLUMNS = {
    "ts",
    "station_id",
    "price_per_kwh",
    "wholesale_per_kwh",
}


def parse_arguments() -> argparse.Namespace:
    assignment_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description=(
            "Convert hourly charger prices into deduplicated, time-ordered "
            "price_update events in JSON Lines format."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=assignment_dir / "data/generated/dc_data/prices_hourly.csv",
        help="Path to prices_hourly.csv.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=assignment_dir / "data/events/price_events.jsonl",
        help="Output JSONL path.",
    )
    parser.add_argument(
        "--start",
        default="2026-09-15T00:55:00",
        help="Inclusive replay-window start in ISO 8601 format.",
    )
    parser.add_argument(
        "--end",
        default="2026-09-15T01:05:00",
        help="Exclusive replay-window end in ISO 8601 format.",
    )
    return parser.parse_args()


def parse_timestamp(value: str, row_number: int) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"Row {row_number}: invalid ISO 8601 timestamp {value!r}."
        ) from exc


def parse_number(value: str, field: str, row_number: int) -> float:
    try:
        number = float(value)
    except ValueError as exc:
        raise ValueError(
            f"Row {row_number}: {field} must be numeric, got {value!r}."
        ) from exc
    if not math.isfinite(number):
        raise ValueError(f"Row {row_number}: {field} must be finite.")
    return number


def load_prices(path: Path) -> list[dict]:
    if not path.is_file():
        raise FileNotFoundError(f"Input file not found: {path}")

    records: list[dict] = []
    seen_keys: set[tuple[str, datetime]] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        columns = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - columns
        if missing:
            raise ValueError(
                "Input CSV is missing required columns: " + ", ".join(sorted(missing))
            )

        for row_number, row in enumerate(reader, start=2):
            station_id = row["station_id"].strip()
            if not station_id:
                raise ValueError(f"Row {row_number}: station_id cannot be empty.")

            timestamp_text = row["ts"].strip()
            timestamp = parse_timestamp(timestamp_text, row_number)
            key = (station_id, timestamp)
            if key in seen_keys:
                raise ValueError(
                    f"Row {row_number}: duplicate station/timestamp pair: "
                    f"{station_id}, {timestamp_text}."
                )
            seen_keys.add(key)

            records.append(
                {
                    "timestamp": timestamp,
                    "event_time": timestamp.isoformat(),
                    "station_id": station_id,
                    "price_per_kwh": parse_number(
                        row["price_per_kwh"].strip(), "price_per_kwh", row_number
                    ),
                    "wholesale_per_kwh": parse_number(
                        row["wholesale_per_kwh"].strip(),
                        "wholesale_per_kwh",
                        row_number,
                    ),
                }
            )

    if not records:
        raise ValueError("Input CSV contains no data rows.")
    return records


def retain_price_changes(records: list[dict]) -> list[dict]:
    """Keep each station's first observation and subsequent actual price changes."""
    by_station: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        by_station[record["station_id"]].append(record)

    retained: list[dict] = []
    for station_records in by_station.values():
        station_records.sort(key=lambda item: item["timestamp"])
        previous_price: float | None = None
        for index, record in enumerate(station_records):
            current_price = record["price_per_kwh"]
            if index == 0 or current_price != previous_price:
                retained.append(record)
            previous_price = current_price
    return retained


def make_event_id(station_id: str, event_time: str) -> str:
    identity = f"price_update|{station_id}|{event_time}"
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
    return f"price_{digest}"


def prepare_events(
    records: list[dict], window_start: datetime, window_end: datetime
) -> tuple[list[dict], int]:
    if window_start >= window_end:
        raise ValueError("Replay-window start must be earlier than end.")

    changed_records = retain_price_changes(records)
    initial = {}
    selected = []
    for record in sorted(changed_records, key=lambda item: item["timestamp"]):
        if record["timestamp"] <= window_start:
            initial[record["station_id"]] = record
        elif record["timestamp"] < window_end:
            selected.append(record)
    selected.extend(initial.values())
    selected.sort(key=lambda item: (item["timestamp"], item["station_id"]))

    events = [
        {
            "event_id": make_event_id(record["station_id"], record["event_time"]),
            "event_type": "price_update",
            "event_time": record["event_time"],
            "station_id": record["station_id"],
            "price_per_kwh": record["price_per_kwh"],
            "wholesale_per_kwh": record["wholesale_per_kwh"],
        }
        for record in selected
    ]

    event_ids = [event["event_id"] for event in events]
    if len(event_ids) != len(set(event_ids)):
        raise ValueError("Generated event_id values are not unique.")
    if not events:
        raise ValueError("No price-change events fall inside the replay window.")

    return events, len(changed_records)


def write_jsonl(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for event in events:
            stream.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")))
            stream.write("\n")


def main() -> None:
    args = parse_arguments()
    window_start = datetime.fromisoformat(args.start)
    window_end = datetime.fromisoformat(args.end)

    records = load_prices(args.input)
    events, all_change_count = prepare_events(records, window_start, window_end)
    write_jsonl(args.output, events)

    station_count = len({record["station_id"] for record in records})
    selected_station_count = len({event["station_id"] for event in events})
    print(f"Input rows: {len(records)}")
    print(f"Input stations: {station_count}")
    print(f"All retained initial/change events: {all_change_count}")
    print(f"Replay window: [{window_start.isoformat()}, {window_end.isoformat()})")
    print(f"Output events: {len(events)} across {selected_station_count} stations")
    print(f"Output file: {args.output}")


if __name__ == "__main__":
    main()
