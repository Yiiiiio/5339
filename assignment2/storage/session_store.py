"""Store and query Assignment 2 session and meter events in DuckDB.

``store_event`` is the single write entry point for the subscriber: it sends
``price_update`` to ``price_store`` and the other three types here. Duplicate
and conflict handling follows the price stage:

- same ``event_id`` and same content -> ``duplicate``;
- same ``event_id`` with different content -> ``ValueError``;
- a second event for the same natural key (``session_id`` + type for session
  events, ``session_id`` + time for meter readings) -> ``duplicate`` when the
  content matches, otherwise ``ValueError``;
- a session's start, end and readings must agree on station and connector.

Events may arrive in any order. State is derived by the ``session_state`` and
``station_session_metrics`` views, never by incrementing counters.
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

import duckdb

from assignment2.storage.price_store import (
    _parse_decimal,
    _parse_timestamp,
    _required_text,
    store_price_event,
)


SESSION_START_FIELDS = {
    "event_id", "event_type", "event_time", "station_id", "connector_id",
    "session_id", "price_per_kwh",
}
SESSION_END_FIELDS = {
    "event_id", "event_type", "event_time", "station_id", "connector_id",
    "session_id", "charge_end_time", "charge_minutes", "energy_kwh", "avg_kw",
    "peak_kw", "price_per_kwh", "cost_aud",
}
METER_FIELDS = {
    "event_id", "event_type", "event_time", "station_id", "connector_id",
    "session_id", "voltage_v", "current_a", "power_kw", "energy_kwh", "soc_pct",
}

SESSION_COLUMNS = (
    "event_type", "session_id", "station_id", "connector_id", "event_ts",
    "vehicle_model", "price_per_kwh", "charge_end_ts", "charge_minutes",
    "energy_kwh", "avg_kw", "peak_kw", "cost_aud",
)
METER_COLUMNS = (
    "session_id", "station_id", "connector_id", "event_ts", "voltage_v",
    "current_a", "power_kw", "energy_kwh", "soc_pct",
)


def _check_fields(event: Mapping[str, Any], required: set[str]) -> None:
    missing = sorted(required.difference(event))
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")


def _connector(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("connector_id must be a positive integer")
    return value


def _double(event: Mapping[str, Any], field: str) -> float:
    return float(_parse_decimal(event[field], field, non_negative=True))


def _optional_text(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string or null")
    return value.strip() or None


def _common(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "event_id": _required_text(event, "event_id"),
        "session_id": _required_text(event, "session_id"),
        "station_id": _required_text(event, "station_id"),
        "connector_id": _connector(event["connector_id"]),
        "event_ts": _parse_timestamp(event["event_time"]),
    }


def _normalise_session_event(event: Mapping[str, Any]) -> dict[str, Any]:
    event_type = _required_text(event, "event_type") if "event_type" in event else None
    if event_type == "session_start":
        _check_fields(event, SESSION_START_FIELDS)
        item = _common(event)
        item.update(
            vehicle_model=_optional_text(event.get("vehicle_model"), "vehicle_model"),
            price_per_kwh=_parse_decimal(event["price_per_kwh"], "price_per_kwh", non_negative=True),
            charge_end_ts=None, charge_minutes=None, energy_kwh=None,
            avg_kw=None, peak_kw=None, cost_aud=None,
        )
    elif event_type == "session_end":
        _check_fields(event, SESSION_END_FIELDS)
        item = _common(event)
        charge_end = _parse_timestamp(event["charge_end_time"])
        if charge_end > item["event_ts"]:
            raise ValueError("charge_end_time must not be after the session_end event_time")
        item.update(
            vehicle_model=None,
            price_per_kwh=_parse_decimal(event["price_per_kwh"], "price_per_kwh", non_negative=True),
            charge_end_ts=charge_end,
            charge_minutes=_double(event, "charge_minutes"),
            energy_kwh=_parse_decimal(event["energy_kwh"], "energy_kwh", non_negative=True),
            avg_kw=_double(event, "avg_kw"),
            peak_kw=_double(event, "peak_kw"),
            cost_aud=_parse_decimal(event["cost_aud"], "cost_aud", non_negative=True),
        )
    else:
        raise ValueError("event_type must be session_start or session_end")
    item["event_type"] = event_type
    item["raw_payload"] = json.dumps(event, ensure_ascii=False, sort_keys=True)
    return item


def _normalise_meter_event(event: Mapping[str, Any]) -> dict[str, Any]:
    _check_fields(event, METER_FIELDS)
    if _required_text(event, "event_type") != "meter":
        raise ValueError("event_type must be meter")
    item = _common(event)
    for field in ("voltage_v", "current_a", "power_kw", "energy_kwh", "soc_pct"):
        item[field] = _double(event, field)
    if item["soc_pct"] > 100:
        raise ValueError("soc_pct must not exceed 100")
    item["raw_payload"] = json.dumps(event, ensure_ascii=False, sort_keys=True)
    return item


def _require_station(connection: duckdb.DuckDBPyConnection, station_id: str) -> None:
    if connection.execute(
        "SELECT 1 FROM charger_records WHERE record_id = ?", [station_id]
    ).fetchone() is None:
        raise ValueError(f"Unknown station_id: {station_id}")


def _require_same_place(
    connection: duckdb.DuckDBPyConnection, item: dict[str, Any]
) -> None:
    """A session's events must all name the same station and connector."""

    rows = connection.execute(
        """
        SELECT station_id, connector_id FROM session_events WHERE session_id = ?
        UNION
        SELECT station_id, connector_id FROM meter_readings WHERE session_id = ?
        """,
        [item["session_id"], item["session_id"]],
    ).fetchall()
    for station_id, connector_id in rows:
        if (station_id, connector_id) != (item["station_id"], item["connector_id"]):
            raise ValueError(
                f"session_id {item['session_id']} already has events for station "
                f"{station_id} connector {connector_id}"
            )


def _store(
    database_path: str | Path,
    item: dict[str, Any],
    table: str,
    columns: tuple[str, ...],
    natural_key: tuple[str, ...],
) -> str:
    expected = tuple(item[column] for column in columns)
    column_list = ", ".join(columns)
    with duckdb.connect(str(Path(database_path))) as connection:
        connection.execute("BEGIN TRANSACTION")
        try:
            _require_station(connection, item["station_id"])

            existing = connection.execute(
                f"SELECT {column_list} FROM {table} WHERE event_id = ?",
                [item["event_id"]],
            ).fetchone()
            if existing is not None:
                if existing == expected:
                    connection.execute("COMMIT")
                    return "duplicate"
                raise ValueError(
                    f"event_id {item['event_id']} already exists with different content"
                )

            key_filter = " AND ".join(f"{column} = ?" for column in natural_key)
            existing = connection.execute(
                f"SELECT {column_list} FROM {table} WHERE {key_filter}",
                [item[column] for column in natural_key],
            ).fetchone()
            if existing is not None:
                if existing == expected:
                    connection.execute("COMMIT")
                    return "duplicate"
                raise ValueError(
                    f"Conflicting {table} content for "
                    + ", ".join(f"{column}={item[column]}" for column in natural_key)
                )

            _require_same_place(connection, item)
            insert_columns = ("event_id", *columns, "raw_payload")
            connection.execute(
                f"INSERT INTO {table} ({', '.join(insert_columns)}) "
                f"VALUES ({', '.join('?' for _ in insert_columns)})",
                [item[column] for column in insert_columns],
            )
            connection.execute("COMMIT")
            return "inserted"
        except Exception:
            connection.execute("ROLLBACK")
            raise


def store_session_event(database_path: str | Path, event: Mapping[str, Any]) -> str:
    """Store one ``session_start`` or ``session_end``; return ``inserted``/``duplicate``."""

    item = _normalise_session_event(event)
    return _store(
        database_path, item, "session_events", SESSION_COLUMNS, ("session_id", "event_type")
    )


def store_meter_event(database_path: str | Path, event: Mapping[str, Any]) -> str:
    """Store one ``meter`` reading; return ``inserted``/``duplicate``."""

    item = _normalise_meter_event(event)
    return _store(
        database_path, item, "meter_readings", METER_COLUMNS, ("session_id", "event_ts")
    )


def store_event(database_path: str | Path, event: Mapping[str, Any]) -> str:
    """Store any of the four event types; the subscriber's single write entry."""

    event_type = event.get("event_type")
    if event_type == "price_update":
        return store_price_event(database_path, event)
    if event_type in ("session_start", "session_end"):
        return store_session_event(database_path, event)
    if event_type == "meter":
        return store_meter_event(database_path, event)
    raise ValueError(f"Unsupported event_type: {event_type!r}")


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _number(value: Any) -> float | None:
    return None if value is None else float(value)


def get_station_session_metrics(
    database_path: str | Path, station_id: str
) -> dict[str, Any]:
    """Current and historical session metrics for one station (zeros if none)."""

    with duckdb.connect(str(Path(database_path)), read_only=True) as connection:
        row = connection.execute(
            """
            SELECT active_sessions, occupied_connectors, completed_sessions,
                   completed_energy_kwh, completed_revenue_aud
            FROM station_session_metrics
            WHERE station_id = ?
            """,
            [station_id],
        ).fetchone()
    row = row or (0, 0, 0, 0, 0)
    return {
        "station_id": station_id,
        "active_sessions": row[0],
        "occupied_connectors": row[1],
        "completed_sessions": row[2],
        "completed_energy_kwh": float(row[3]),
        "completed_revenue_aud": float(row[4]),
    }


def get_station_sessions(
    database_path: str | Path, station_id: str, limit: int = 20
) -> list[dict[str, Any]]:
    """Active sessions (latest start first), then the most recent completed ones."""

    with duckdb.connect(str(Path(database_path)), read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT s.session_id, s.connector_id, s.status, s.start_ts, s.end_ts,
                   s.charge_end_ts, s.vehicle_model, s.start_price_per_kwh,
                   s.energy_kwh, s.cost_aud,
                   coalesce(m.readings, 0) AS readings,
                   m.last_reading_ts
            FROM session_state AS s
            LEFT JOIN (
                SELECT session_id, count(*) AS readings, max(event_ts) AS last_reading_ts
                FROM meter_readings
                WHERE station_id = $1
                GROUP BY session_id
            ) AS m ON m.session_id = s.session_id
            WHERE s.station_id = $1
            ORDER BY s.status = 'active' DESC,
                     coalesce(s.end_ts, s.start_ts) DESC,
                     s.session_id
            LIMIT $2
            """,
            [station_id, limit],
        ).fetchall()
    return [
        {
            "session_id": row[0],
            "connector_id": row[1],
            "status": row[2],
            "start_time": _iso(row[3]),
            "end_time": _iso(row[4]),
            "charge_end_time": _iso(row[5]),
            "vehicle_model": row[6],
            "start_price_per_kwh": _number(row[7]),
            "energy_kwh": _number(row[8]),
            "cost_aud": _number(row[9]),
            "readings": row[10],
            "last_reading_time": _iso(row[11]),
        }
        for row in rows
    ]


def get_session_readings(
    database_path: str | Path, session_id: str
) -> list[dict[str, Any]]:
    """One session's received meter readings in event-time order."""

    with duckdb.connect(str(Path(database_path)), read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT event_ts, voltage_v, current_a, power_kw, energy_kwh, soc_pct
            FROM meter_readings
            WHERE session_id = ?
            ORDER BY event_ts
            """,
            [session_id],
        ).fetchall()
    return [
        {
            "event_time": row[0].isoformat(),
            "voltage_v": row[1],
            "current_a": row[2],
            "power_kw": row[3],
            "energy_kwh": row[4],
            "soc_pct": row[5],
        }
        for row in rows
    ]
