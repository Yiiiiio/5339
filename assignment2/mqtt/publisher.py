#!/usr/bin/env python3
"""Publish prepared EV charger price-update events to an MQTT broker.

The program reads one JSON object per line from ``price_events.jsonl``, checks
the event schema and chronological order, and publishes each event to
``evchargers/{station_id}/price`` using MQTT QoS 1. A delay of at least
0.1 seconds is enforced between consecutive messages.

Example:
    python publisher.py --events ../data/events/price_events.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import datetime
from pathlib import Path
from threading import Event
from typing import Any

import paho.mqtt.client as mqtt


REQUIRED_FIELDS = {
    "event_id",
    "event_type",
    "event_time",
    "station_id",
    "price_per_kwh",
    "wholesale_per_kwh",
}
MINIMUM_INTERVAL_SECONDS = 0.1


def parse_arguments() -> argparse.Namespace:
    assignment_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Publish prepared EV charger price_update events via MQTT."
    )
    parser.add_argument(
        "--events",
        type=Path,
        default=assignment_dir / "data/events/price_events.jsonl",
        help="Input JSONL event file.",
    )
    parser.add_argument("--host", default="localhost", help="MQTT Broker host.")
    parser.add_argument("--port", type=int, default=1883, help="MQTT Broker port.")
    parser.add_argument(
        "--topic-prefix",
        default="evchargers",
        help="Prefix for evchargers/{station_id}/price topics.",
    )
    parser.add_argument(
        "--client-id",
        default="comp5339-price-publisher",
        help="MQTT client identifier.",
    )
    parser.add_argument("--username", help="Optional MQTT username.")
    parser.add_argument("--password", help="Optional MQTT password.")
    parser.add_argument(
        "--interval",
        type=float,
        default=MINIMUM_INTERVAL_SECONDS,
        help="Seconds between messages; must be at least 0.1.",
    )
    parser.add_argument(
        "--publish-timeout",
        type=float,
        default=10.0,
        help="Seconds to wait for each QoS 1 publish acknowledgement.",
    )
    args = parser.parse_args()

    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if args.interval < MINIMUM_INTERVAL_SECONDS:
        parser.error("--interval must be at least 0.1 seconds")
    if args.publish_timeout <= 0:
        parser.error("--publish-timeout must be positive")
    if bool(args.username) != bool(args.password):
        parser.error("--username and --password must be supplied together")
    if not args.topic_prefix or any(char in args.topic_prefix for char in "+#"):
        parser.error("--topic-prefix cannot be empty or contain MQTT wildcards")
    return args


def require_nonempty_string(event: dict[str, Any], field: str, line: int) -> str:
    value = event[field]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Line {line}: {field} must be a non-empty string")
    return value.strip()


def require_finite_number(event: dict[str, Any], field: str, line: int) -> float:
    value = event[field]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Line {line}: {field} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"Line {line}: {field} must be finite")
    return number


def validate_event(raw_event: Any, line: int) -> tuple[dict[str, Any], datetime]:
    """Validate one JSONL record and return its normalized event and timestamp."""
    if not isinstance(raw_event, dict):
        raise ValueError(f"Line {line}: event must be a JSON object")
    missing = REQUIRED_FIELDS - raw_event.keys()
    if missing:
        raise ValueError(
            f"Line {line}: missing required fields: {', '.join(sorted(missing))}"
        )

    event_id = require_nonempty_string(raw_event, "event_id", line)
    event_type = require_nonempty_string(raw_event, "event_type", line)
    event_time = require_nonempty_string(raw_event, "event_time", line)
    station_id = require_nonempty_string(raw_event, "station_id", line)
    if event_type != "price_update":
        raise ValueError(f"Line {line}: unsupported event_type {event_type!r}")
    if any(char in station_id for char in "/+#"):
        raise ValueError(
            f"Line {line}: station_id cannot contain '/', '+' or '#' in an MQTT topic"
        )
    try:
        timestamp = datetime.fromisoformat(event_time)
    except ValueError as exc:
        raise ValueError(
            f"Line {line}: event_time must be an ISO 8601 timestamp"
        ) from exc

    price = require_finite_number(raw_event, "price_per_kwh", line)
    wholesale = require_finite_number(raw_event, "wholesale_per_kwh", line)
    if price < 0:
        raise ValueError(f"Line {line}: price_per_kwh cannot be negative")

    normalized = {
        "event_id": event_id,
        "event_type": event_type,
        "event_time": event_time,
        "station_id": station_id,
        "price_per_kwh": price,
        "wholesale_per_kwh": wholesale,
    }
    return normalized, timestamp


def load_events(path: Path) -> list[dict[str, Any]]:
    """Load JSONL events and reject duplicates or non-chronological input."""
    if not path.is_file():
        raise FileNotFoundError(f"Event file not found: {path}")

    events: list[dict[str, Any]] = []
    timestamps: list[datetime] = []
    event_ids: set[str] = set()
    with path.open("r", encoding="utf-8") as stream:
        for line_number, line_text in enumerate(stream, start=1):
            if not line_text.strip():
                continue
            try:
                raw_event = json.loads(line_text)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Line {line_number}: invalid JSON: {exc.msg}"
                ) from exc
            event, timestamp = validate_event(raw_event, line_number)
            if event["event_id"] in event_ids:
                raise ValueError(
                    f"Line {line_number}: duplicate event_id {event['event_id']!r}"
                )
            event_ids.add(event["event_id"])
            events.append(event)
            timestamps.append(timestamp)

    if not events:
        raise ValueError("Event file contains no events")
    if timestamps != sorted(timestamps):
        raise ValueError("Events are not sorted by event_time")
    return events


class PricePublisher:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.connected = Event()
        self.connection_error: str | None = None
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=args.client_id,
            protocol=mqtt.MQTTv311,
        )
        if args.username:
            self.client.username_pw_set(args.username, args.password)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect

    def on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del client, userdata, flags, properties
        if reason_code.is_failure:
            self.connection_error = str(reason_code)
        self.connected.set()

    def on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del client, userdata, disconnect_flags, properties
        if reason_code.is_failure:
            print(f"Unexpected disconnect: {reason_code}", file=sys.stderr, flush=True)

    def run(self, events: list[dict[str, Any]]) -> int:
        print(f"Connecting to MQTT Broker at {self.args.host}:{self.args.port}...", flush=True)
        try:
            self.client.connect(self.args.host, self.args.port, keepalive=60)
        except OSError as exc:
            print(f"Connection failed: {exc}", file=sys.stderr)
            return 1

        self.client.loop_start()
        sent = 0
        failed = 0
        try:
            if not self.connected.wait(timeout=10):
                print("Connection timed out", file=sys.stderr)
                return 1
            if self.connection_error:
                print(f"Connection rejected: {self.connection_error}", file=sys.stderr)
                return 1

            print(f"Connected; publishing {len(events)} events with QoS 1", flush=True)
            for index, event in enumerate(events, start=1):
                topic = (
                    f"{self.args.topic_prefix.rstrip('/')}/"
                    f"{event['station_id']}/price"
                )
                payload = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
                message_info = self.client.publish(topic, payload, qos=1, retain=False)
                if message_info.rc != mqtt.MQTT_ERR_SUCCESS:
                    failed += 1
                    print(
                        f"FAILED #{index} event_id={event['event_id']}: "
                        f"{mqtt.error_string(message_info.rc)}",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue

                message_info.wait_for_publish(timeout=self.args.publish_timeout)
                if not message_info.is_published():
                    failed += 1
                    print(
                        f"FAILED #{index} event_id={event['event_id']}: "
                        "publish acknowledgement timed out",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue

                sent += 1
                print(
                    "PUBLISHED "
                    f"#{index} event_id={event['event_id']} "
                    f"station={event['station_id']} time={event['event_time']} "
                    f"price={event['price_per_kwh']:.2f}",
                    flush=True,
                )
                if index < len(events):
                    time.sleep(self.args.interval)
        finally:
            self.client.disconnect()
            self.client.loop_stop()

        print(f"Finished. published={sent} failed={failed}", flush=True)
        return 0 if failed == 0 else 1


def main() -> int:
    args = parse_arguments()
    try:
        events = load_events(args.events)
    except (FileNotFoundError, ValueError) as exc:
        print(f"Cannot load events: {exc}", file=sys.stderr)
        return 1
    publisher = PricePublisher(args)
    return publisher.run(events)


if __name__ == "__main__":
    raise SystemExit(main())
