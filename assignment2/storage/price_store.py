"""Store and query Assignment 2 ``price_update`` events in DuckDB.

The first project stage deliberately handles only price events. Session and
meter events will be added after the price path is working end to end.
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

import duckdb

from assignment2.storage.connection import open_database


DEFAULT_SCHEMA_PATH = Path(__file__).resolve().parents[1] / "sql" / "a2_schema.sql"
REQUIRED_FIELDS = {
    "event_id",
    "event_type",
    "station_id",
    "event_time",
    "price_per_kwh",
    "wholesale_per_kwh",
}


def initialise_database(
    database_path: str | Path,
    schema_path: str | Path = DEFAULT_SCHEMA_PATH,
) -> None:
    """Create the A2 price table and latest-price view if they do not exist."""

    schema = Path(schema_path).read_text(encoding="utf-8")
    with open_database(database_path) as connection:
        connection.execute(schema)


def _required_text(event: Mapping[str, Any], field: str) -> str:
    value = event[field]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def _parse_timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("event_time must be a non-empty ISO timestamp string")
    try:
        timestamp = datetime.fromisoformat(value.strip())
    except ValueError as error:
        raise ValueError(f"Invalid event_time value: {value}") from error
    if timestamp.tzinfo is not None:
        raise ValueError(
            "event_time must not contain a timezone until the team agrees on one interpretation"
        )
    return timestamp


def _parse_decimal(value: Any, field: str, *, non_negative: bool) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"{field} must be numeric") from error
    if not number.is_finite():
        raise ValueError(f"{field} must be finite")
    if non_negative and number < 0:
        raise ValueError(f"{field} must not be negative")
    return number


def _normalise_event(event: Mapping[str, Any]) -> dict[str, Any]:
    missing = sorted(REQUIRED_FIELDS.difference(event))
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    event_id = _required_text(event, "event_id")
    event_type = _required_text(event, "event_type")
    if event_type != "price_update":
        raise ValueError("event_type must be price_update")

    return {
        "event_id": event_id,
        "event_type": event_type,
        "station_id": _required_text(event, "station_id"),
        "event_ts": _parse_timestamp(event["event_time"]),
        "price_per_kwh": _parse_decimal(
            event["price_per_kwh"], "price_per_kwh", non_negative=True
        ),
        # Wholesale prices may legitimately be negative.
        "wholesale_per_kwh": _parse_decimal(
            event["wholesale_per_kwh"],
            "wholesale_per_kwh",
            non_negative=False,
        ),
        "raw_payload": json.dumps(event, ensure_ascii=False, sort_keys=True),
    }


def store_price_event(database_path: str | Path, event: Mapping[str, Any]) -> str:
    """Store one price event and return ``inserted`` or ``duplicate``.

    Reusing an event ID with different content, sending two different prices for
    the same station and timestamp, or referencing an unknown station raises a
    ``ValueError`` instead of silently changing existing data.
    """

    item = _normalise_event(event)
    with open_database(database_path) as connection:
        connection.execute("BEGIN TRANSACTION")
        try:
            station_exists = connection.execute(
                "SELECT 1 FROM charger_records WHERE record_id = ?",
                [item["station_id"]],
            ).fetchone()
            if station_exists is None:
                raise ValueError(f"Unknown station_id: {item['station_id']}")

            existing_id = connection.execute(
                """
                SELECT station_id, event_ts, price_per_kwh, wholesale_per_kwh
                FROM price_events
                WHERE event_id = ?
                """,
                [item["event_id"]],
            ).fetchone()
            expected = (
                item["station_id"],
                item["event_ts"],
                item["price_per_kwh"],
                item["wholesale_per_kwh"],
            )
            if existing_id is not None:
                if existing_id == expected:
                    connection.execute("COMMIT")
                    return "duplicate"
                raise ValueError(
                    f"event_id {item['event_id']} already exists with different content"
                )

            existing_time = connection.execute(
                """
                SELECT event_id, price_per_kwh, wholesale_per_kwh
                FROM price_events
                WHERE station_id = ? AND event_ts = ?
                """,
                [item["station_id"], item["event_ts"]],
            ).fetchone()
            if existing_time is not None:
                if existing_time[1:] == expected[2:]:
                    connection.execute("COMMIT")
                    return "duplicate"
                raise ValueError(
                    "Conflicting price values for the same station_id and event_time"
                )

            connection.execute(
                """
                INSERT INTO price_events (
                    event_id,
                    event_type,
                    station_id,
                    event_ts,
                    price_per_kwh,
                    wholesale_per_kwh,
                    raw_payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    item["event_id"],
                    item["event_type"],
                    item["station_id"],
                    item["event_ts"],
                    item["price_per_kwh"],
                    item["wholesale_per_kwh"],
                    item["raw_payload"],
                ],
            )
            connection.execute("COMMIT")
            return "inserted"
        except Exception:
            connection.execute("ROLLBACK")
            raise


def _price_row_to_dict(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "event_id": row[0],
        "station_id": row[1],
        "event_time": row[2].isoformat(),
        "price_per_kwh": float(row[3]),
        "wholesale_per_kwh": None if row[4] is None else float(row[4]),
        "received_at": row[5].isoformat(),
    }


def get_latest_price(
    database_path: str | Path, station_id: str
) -> dict[str, Any] | None:
    """Return the latest received price for one station, or ``None``."""

    with open_database(database_path, read_only=True) as connection:
        row = connection.execute(
            """
            SELECT event_id, station_id, event_ts, price_per_kwh,
                   wholesale_per_kwh, received_at
            FROM latest_station_price
            WHERE station_id = ?
            """,
            [station_id],
        ).fetchone()
    return None if row is None else _price_row_to_dict(row)


def get_price_history(
    database_path: str | Path, station_id: str
) -> list[dict[str, Any]]:
    """Return one station's complete price history in event-time order."""

    with open_database(database_path, read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT event_id, station_id, event_ts, price_per_kwh,
                   wholesale_per_kwh, received_at
            FROM price_events
            WHERE station_id = ?
            ORDER BY event_ts, received_at, event_id
            """,
            [station_id],
        ).fetchall()
    return [_price_row_to_dict(row) for row in rows]

