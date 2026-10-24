#!/usr/bin/env python3
"""Prepare one time-ordered replay of all four event types for a time window.

Window rules (half-open ``[start, end)``):

- Prices: each station's effective price at ``start`` (its latest retained
  initial/change event at or before ``start``) plus every price change inside
  the window. B's price filtering in ``prepare_price_events`` is reused.
- Sessions: every session that overlaps the window (``start_ts < end`` and
  ``end_ts >= start``) contributes its ``session_start`` and all of its meter
  readings before ``end``, even if they precede ``start``, so a session that
  is already charging at the window start is not shown as idle and its curve
  is complete. ``session_end`` is included only if ``end_ts < end``.

Events keep their original event times; nothing is shifted or rewritten.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from assignment2.mqtt.event_schema import EVENT_ORDER, validate_event
from assignment2.mqtt.prepare_price_events import (
    load_prices,
    make_event_id as make_price_event_id,
    parse_number,
    parse_timestamp,
    retain_price_changes,
    write_jsonl,
)


SESSION_COLUMNS = {
    "session_id", "station_id", "connector_id", "vehicle_model", "start_ts",
    "charge_end_ts", "end_ts", "charge_minutes", "energy_kwh", "avg_kw",
    "peak_kw", "price_per_kwh", "cost_aud",
}
METER_COLUMNS = {
    "ts", "station_id", "connector_id", "session_id", "voltage_v",
    "current_a", "power_kw", "energy_kwh", "soc_pct",
}


def parse_arguments() -> argparse.Namespace:
    assignment_dir = Path(__file__).resolve().parents[1]
    generated = assignment_dir / "data/generated/dc_data"
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--prices", type=Path, default=generated / "prices_hourly.csv")
    parser.add_argument("--sessions", type=Path, default=generated / "sessions.csv")
    parser.add_argument("--metering", type=Path, default=generated / "session_metering.csv")
    parser.add_argument(
        "--output",
        type=Path,
        default=assignment_dir / "data/events/replay_events.jsonl",
        help="Output JSONL path.",
    )
    parser.add_argument("--start", default="2026-09-15T00:55:00", help="Inclusive window start.")
    parser.add_argument("--end", default="2026-09-15T01:05:00", help="Exclusive window end.")
    return parser.parse_args()


def make_event_id(event_type: str, key: str, event_time: str) -> str:
    digest = hashlib.sha256(f"{event_type}|{key}|{event_time}".encode("utf-8")).hexdigest()[:24]
    return f"{event_type}_{digest}"


def read_csv(path: Path, columns: set[str]) -> list[tuple[int, dict[str, str]]]:
    if not path.is_file():
        raise FileNotFoundError(f"Input file not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        missing = columns - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path} is missing columns: {', '.join(sorted(missing))}")
        return [(number, row) for number, row in enumerate(reader, start=2)]


def _connector(value: str, row_number: int) -> int:
    try:
        connector = int(value)
    except ValueError as exc:
        raise ValueError(f"Row {row_number}: connector_id must be an integer") from exc
    if connector < 1:
        raise ValueError(f"Row {row_number}: connector_id must be positive")
    return connector


def price_events(records: list[dict], start: datetime, end: datetime) -> list[dict]:
    changes = retain_price_changes(records)
    selected = {}
    for record in sorted(changes, key=lambda item: item["timestamp"]):
        if record["timestamp"] <= start:
            selected[record["station_id"]] = [record]
        elif record["timestamp"] < end:
            selected.setdefault(record["station_id"], []).append(record)
    return [
        {
            "event_id": make_price_event_id(record["station_id"], record["event_time"]),
            "event_type": "price_update",
            "event_time": record["event_time"],
            "station_id": record["station_id"],
            "price_per_kwh": record["price_per_kwh"],
            "wholesale_per_kwh": record["wholesale_per_kwh"],
        }
        for records_for_station in selected.values()
        for record in records_for_station
    ]


def session_events(
    session_rows: list[tuple[int, dict[str, str]]],
    meter_rows: list[tuple[int, dict[str, str]]],
    start: datetime,
    end: datetime,
) -> list[dict]:
    events = []
    overlapping: dict[str, tuple[str, int]] = {}
    for row_number, row in session_rows:
        session_id = row["session_id"].strip()
        station_id = row["station_id"].strip()
        if not session_id or not station_id:
            raise ValueError(f"Row {row_number}: session_id and station_id are required")
        if session_id in overlapping:
            raise ValueError(f"Row {row_number}: duplicate session_id {session_id}")
        started = parse_timestamp(row["start_ts"].strip(), row_number)
        charge_end = parse_timestamp(row["charge_end_ts"].strip(), row_number)
        ended = parse_timestamp(row["end_ts"].strip(), row_number)
        if not started <= charge_end <= ended:
            raise ValueError(f"Row {row_number}: expected start_ts <= charge_end_ts <= end_ts")
        if not (started < end and ended >= start):
            continue
        connector = _connector(row["connector_id"].strip(), row_number)
        overlapping[session_id] = (station_id, connector)
        common = {"station_id": station_id, "connector_id": connector, "session_id": session_id}
        events.append(
            {
                "event_id": make_event_id("session_start", session_id, started.isoformat()),
                "event_type": "session_start",
                "event_time": started.isoformat(),
                **common,
                "vehicle_model": row["vehicle_model"].strip() or None,
                "price_per_kwh": parse_number(row["price_per_kwh"], "price_per_kwh", row_number),
            }
        )
        if ended < end:
            numbers = {
                field: parse_number(row[field], field, row_number)
                for field in ("charge_minutes", "energy_kwh", "avg_kw", "peak_kw",
                              "price_per_kwh", "cost_aud")
            }
            events.append(
                {
                    "event_id": make_event_id("session_end", session_id, ended.isoformat()),
                    "event_type": "session_end",
                    "event_time": ended.isoformat(),
                    **common,
                    "charge_end_time": charge_end.isoformat(),
                    **numbers,
                }
            )

    for row_number, row in meter_rows:
        session_id = row["session_id"].strip()
        if session_id not in overlapping:
            continue
        timestamp = parse_timestamp(row["ts"].strip(), row_number)
        if timestamp >= end:
            continue
        station_id, connector = overlapping[session_id]
        if row["station_id"].strip() != station_id or _connector(
            row["connector_id"].strip(), row_number
        ) != connector:
            raise ValueError(f"Row {row_number}: meter station/connector differs from its session")
        events.append(
            {
                "event_id": make_event_id("meter", session_id, timestamp.isoformat()),
                "event_type": "meter",
                "event_time": timestamp.isoformat(),
                "station_id": station_id,
                "connector_id": connector,
                "session_id": session_id,
                **{
                    field: parse_number(row[field], field, row_number)
                    for field in ("voltage_v", "current_a", "power_kw", "energy_kwh", "soc_pct")
                },
            }
        )
    return events


def prepare_replay(
    prices_path: Path,
    sessions_path: Path,
    metering_path: Path,
    start: datetime,
    end: datetime,
) -> list[dict]:
    if start >= end:
        raise ValueError("Replay-window start must be earlier than end.")
    events = price_events(load_prices(prices_path), start, end)
    events += session_events(
        read_csv(sessions_path, SESSION_COLUMNS),
        read_csv(metering_path, METER_COLUMNS),
        start,
        end,
    )
    for event in events:
        validate_event(event)
    events.sort(
        key=lambda event: (
            event["event_time"],
            EVENT_ORDER[event["event_type"]],
            event["station_id"],
            event.get("session_id", ""),
        )
    )
    event_ids = [event["event_id"] for event in events]
    if len(event_ids) != len(set(event_ids)):
        raise ValueError("Generated event_id values are not unique.")
    if not events:
        raise ValueError("No events fall inside the replay window.")
    return events


def main() -> int:
    args = parse_arguments()
    start = datetime.fromisoformat(args.start)
    end = datetime.fromisoformat(args.end)
    try:
        events = prepare_replay(args.prices, args.sessions, args.metering, start, end)
    except (OSError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    write_jsonl(args.output, events)

    counts = Counter(event["event_type"] for event in events)
    before = Counter(event["event_type"] for event in events if event["event_time"] < start.isoformat())
    sessions = defaultdict(set)
    for event in events:
        if "session_id" in event:
            sessions[event["event_type"]].add(event["session_id"])
    print(f"Replay window: [{start.isoformat()}, {end.isoformat()})")
    for event_type in EVENT_ORDER:
        print(f"{event_type}: {counts[event_type]} (before window start: {before[event_type]})")
    print(f"Sessions overlapping window: {len(sessions['session_start'])}; "
          f"ending inside window: {len(sessions['session_end'])}")
    print(f"Total events: {len(events)} -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
