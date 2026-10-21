#!/usr/bin/env python3
"""Publish prepared EV charger events to an MQTT broker.

The program reads one JSON object per line (``price_events.jsonl`` or the
four-type ``replay_events.jsonl``), checks the event schema and chronological
order, and publishes each event to ``evchargers/{station_id}/{suffix}``
(suffix ``price``, ``session_start``, ``meter`` or ``session_end``) using MQTT
QoS 1. A delay of at least 0.1 seconds is enforced between consecutive messages.

Example:
    python publisher.py --events ../data/events/replay_events.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from threading import Event
from typing import Any

import paho.mqtt.client as mqtt

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from assignment2.mqtt.event_schema import describe, topic_for, validate_event as validate_schema


MINIMUM_INTERVAL_SECONDS = 0.1


def parse_arguments() -> argparse.Namespace:
    assignment_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Publish prepared EV charger events via MQTT."
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
        help="Prefix for evchargers/{station_id}/{suffix} topics.",
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


def validate_event(raw_event: Any, line: int) -> tuple[dict[str, Any], datetime]:
    """Validate one JSONL record and return its normalized event and timestamp."""
    try:
        return validate_schema(raw_event)
    except ValueError as exc:
        raise ValueError(f"Line {line}: {exc}") from exc


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
                topic = topic_for(self.args.topic_prefix, event)
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
                    f"PUBLISHED #{index} event_id={event['event_id']} {describe(event)}",
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
