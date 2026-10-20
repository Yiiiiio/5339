#!/usr/bin/env python3
"""Subscribe to MQTT price updates, validate them, and log accepted events."""

from __future__ import annotations

import argparse
import json
import math
import signal
import sys
from datetime import datetime
from pathlib import Path
from threading import Event, Lock
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


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Subscribe to and validate EV charger price_update events."
    )
    parser.add_argument("--host", default="localhost", help="MQTT Broker host.")
    parser.add_argument("--port", type=int, default=1883, help="MQTT Broker port.")
    parser.add_argument(
        "--topic",
        default="evchargers/+/price",
        help="MQTT subscription topic filter.",
    )
    parser.add_argument(
        "--client-id",
        default="comp5339-price-subscriber",
        help="MQTT client identifier.",
    )
    parser.add_argument("--username", help="Optional MQTT username.")
    parser.add_argument("--password", help="Optional MQTT password.")
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional JSONL file for accepted events.",
    )
    parser.add_argument(
        "--database",
        type=Path,
        help="Existing A2 copy of the A1 DuckDB database; enables price storage.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after N accepted events (includes stored duplicates in database mode); 0 waits.",
    )
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if args.max_messages < 0:
        parser.error("--max-messages cannot be negative")
    if bool(args.username) != bool(args.password):
        parser.error("--username and --password must be supplied together")
    return args


def require_nonempty_string(event: dict[str, Any], field: str) -> str:
    value = event[field]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def require_finite_number(event: dict[str, Any], field: str) -> float:
    value = event[field]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field} must be finite")
    return number


def station_from_topic(topic: str) -> str:
    parts = topic.split("/")
    if len(parts) != 3 or parts[0] != "evchargers" or parts[2] != "price":
        raise ValueError(f"unexpected topic structure: {topic!r}")
    if not parts[1]:
        raise ValueError("topic station_id cannot be empty")
    return parts[1]


def validate_event(payload: bytes, topic: str) -> dict[str, Any]:
    try:
        decoded = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("payload is not valid UTF-8") from exc

    try:
        event = json.loads(decoded)
    except json.JSONDecodeError as exc:
        raise ValueError(f"payload is not valid JSON: {exc.msg}") from exc

    if not isinstance(event, dict):
        raise ValueError("payload must be a JSON object")
    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        raise ValueError("missing required fields: " + ", ".join(sorted(missing)))

    event_id = require_nonempty_string(event, "event_id")
    event_type = require_nonempty_string(event, "event_type")
    event_time = require_nonempty_string(event, "event_time")
    station_id = require_nonempty_string(event, "station_id")
    if event_type != "price_update":
        raise ValueError(f"unsupported event_type: {event_type!r}")
    try:
        datetime.fromisoformat(event_time)
    except ValueError as exc:
        raise ValueError("event_time must be an ISO 8601 timestamp") from exc

    topic_station = station_from_topic(topic)
    if topic_station != station_id:
        raise ValueError(
            f"topic station_id {topic_station!r} does not match payload {station_id!r}"
        )

    price = require_finite_number(event, "price_per_kwh")
    wholesale = require_finite_number(event, "wholesale_per_kwh")
    if price < 0:
        raise ValueError("price_per_kwh cannot be negative")

    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_time": event_time,
        "station_id": station_id,
        "price_per_kwh": price,
        "wholesale_per_kwh": wholesale,
    }


class PriceSubscriber:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.stop_event = Event()
        self.file_lock = Lock()
        self.seen_event_ids: set[str] = set()
        self.accepted_count = 0
        self.rejected_count = 0
        self.duplicate_count = 0
        self.storage_error_count = 0
        self.store_event = None
        self.storage_errors: tuple[type[Exception], ...] = (ValueError, OSError)
        self.database_path = getattr(args, "database", None)
        if self.database_path is not None:
            self.database_path = Path(self.database_path).resolve()
            if not self.database_path.is_file():
                raise FileNotFoundError(
                    f"A2 database does not exist: {self.database_path}. "
                    "Copy the A1 database first."
                )
            # Support both `python -m ...` and the documented script command.
            if not __package__:
                sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
            import duckdb
            from assignment2.storage.price_store import (
                initialise_database,
                store_price_event,
            )

            initialise_database(self.database_path)
            self.store_event = store_price_event
            self.storage_errors = (ValueError, OSError, duckdb.Error)

        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=args.client_id,
            protocol=mqtt.MQTTv311,
        )
        if args.username:
            self.client.username_pw_set(args.username, args.password)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

    def on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del userdata, flags, properties
        if reason_code.is_failure:
            print(f"Connection rejected: {reason_code}", file=sys.stderr, flush=True)
            self.stop_event.set()
            return
        result, message_id = client.subscribe(self.args.topic, qos=1)
        if result != mqtt.MQTT_ERR_SUCCESS:
            print(f"Subscription failed: {mqtt.error_string(result)}", file=sys.stderr)
            self.stop_event.set()
            return
        print(
            f"Connected to {self.args.host}:{self.args.port}; "
            f"subscribing to {self.args.topic} (mid={message_id})",
            flush=True,
        )

    def on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del client, userdata, disconnect_flags, properties
        if not self.stop_event.is_set() and reason_code.is_failure:
            print(f"Unexpected disconnect: {reason_code}", file=sys.stderr, flush=True)

    def on_message(
        self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage
    ) -> None:
        del client, userdata
        try:
            event = validate_event(message.payload, message.topic)
        except ValueError as exc:
            self.rejected_count += 1
            print(f"REJECT topic={message.topic}: {exc}", file=sys.stderr, flush=True)
            return

        event_id = event["event_id"]
        storage_result = None
        if self.store_event is not None:
            try:
                storage_result = self.store_event(self.database_path, event)
            except self.storage_errors as exc:
                self.storage_error_count += 1
                print(f"STORE_ERROR event_id={event_id}: {exc}", file=sys.stderr, flush=True)
                return
        elif event_id in self.seen_event_ids:
            self.duplicate_count += 1
            print(f"DUPLICATE event_id={event_id}; ignored", flush=True)
            return
        if storage_result == "duplicate":
            self.duplicate_count += 1
            print(f"DUPLICATE event_id={event_id}; already stored", flush=True)
            self.check_message_limit()
            return
        self.seen_event_ids.add(event_id)

        if self.args.output:
            try:
                self.args.output.parent.mkdir(parents=True, exist_ok=True)
                with self.file_lock, self.args.output.open("a", encoding="utf-8") as stream:
                    stream.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")))
                    stream.write("\n")
            except OSError as exc:
                print(f"LOG_ERROR event_id={event_id}: {exc}", file=sys.stderr, flush=True)
                self.stop_event.set()
                self.storage_error_count += 1

        self.accepted_count += 1
        print(
            "RECEIVED "
            f"#{self.accepted_count} event_id={event_id} "
            f"station={event['station_id']} time={event['event_time']} "
            f"price={event['price_per_kwh']:.2f}",
            flush=True,
        )
        if storage_result == "inserted":
            print(f"STORED event_id={event_id}", flush=True)

        self.check_message_limit()

    def check_message_limit(self) -> None:
        # A full replay of already stored events must also be able to stop.
        completed = self.accepted_count
        if self.store_event is not None:
            completed += self.duplicate_count
        if self.args.max_messages and completed >= self.args.max_messages:
            self.stop_event.set()

    def run(self) -> int:
        print(f"Connecting to MQTT Broker at {self.args.host}:{self.args.port}...", flush=True)
        try:
            self.client.connect(self.args.host, self.args.port, keepalive=60)
        except OSError as exc:
            print(f"Connection failed: {exc}", file=sys.stderr)
            return 1

        self.client.loop_start()
        try:
            self.stop_event.wait()
        finally:
            self.stop_event.set()
            self.client.disconnect()
            self.client.loop_stop()
            print(
                f"Stopped. accepted={self.accepted_count} "
                f"duplicates={self.duplicate_count} rejected={self.rejected_count} "
                f"storage_errors={self.storage_error_count}",
                flush=True,
            )
        return 1 if self.storage_error_count else 0


def main() -> int:
    args = parse_arguments()
    subscriber = PriceSubscriber(args)

    def request_stop(signum: int, frame: Any) -> None:
        del signum, frame
        subscriber.stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    return subscriber.run()


if __name__ == "__main__":
    raise SystemExit(main())
