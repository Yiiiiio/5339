"""Shared message schema for the four Assignment 2 MQTT event types.

Each event is published to ``{prefix}/{station_id}/{topic suffix}``. Only
information known at the event time is included: a ``session_start`` carries
no final energy or cost; those arrive with ``session_end``.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any


TOPIC_SUFFIX = {
    "price_update": "price",
    "session_start": "session_start",
    "meter": "meter",
    "session_end": "session_end",
}
EVENT_TYPE_BY_SUFFIX = {suffix: event_type for event_type, suffix in TOPIC_SUFFIX.items()}

# Processing order for events that share one event_time: a session's start
# precedes its readings, and its last reading precedes its end.
EVENT_ORDER = {event_type: rank for rank, event_type in enumerate(TOPIC_SUFFIX)}

COMMON_TEXT_FIELDS = ("event_id", "event_type", "event_time", "station_id")
SESSION_TEXT_FIELDS = ("session_id",)

# field -> minimum allowed value (None = any finite number)
NUMERIC_FIELDS: dict[str, dict[str, float | None]] = {
    "price_update": {"price_per_kwh": 0.0, "wholesale_per_kwh": None},
    "session_start": {"price_per_kwh": 0.0},
    "meter": {
        "voltage_v": 0.0,
        "current_a": 0.0,
        "power_kw": 0.0,
        "energy_kwh": 0.0,
        "soc_pct": 0.0,
    },
    "session_end": {
        "charge_minutes": 0.0,
        "energy_kwh": 0.0,
        "avg_kw": 0.0,
        "peak_kw": 0.0,
        "price_per_kwh": 0.0,
        "cost_aud": 0.0,
    },
}
OPTIONAL_TEXT_FIELDS = {"session_start": ("vehicle_model",)}
TIME_FIELDS = {"session_end": ("charge_end_time",)}


def required_fields(event_type: str) -> set[str]:
    fields = set(COMMON_TEXT_FIELDS) | set(NUMERIC_FIELDS[event_type])
    if event_type != "price_update":
        fields |= set(SESSION_TEXT_FIELDS) | {"connector_id"}
    fields |= set(TIME_FIELDS.get(event_type, ()))
    return fields


def _text(event: dict[str, Any], field: str) -> str:
    value = event[field]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def _number(event: dict[str, Any], field: str, minimum: float | None) -> float:
    value = event[field]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field} must be finite")
    if minimum is not None and number < minimum:
        raise ValueError(f"{field} cannot be below {minimum:g}")
    return number


def _timestamp(event: dict[str, Any], field: str) -> datetime:
    text = _text(event, field)
    try:
        timestamp = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO 8601 timestamp") from exc
    if timestamp.tzinfo is not None:
        raise ValueError(f"{field} must be timezone-naive, as in the generator output")
    return timestamp


def validate_event(event: Any) -> tuple[dict[str, Any], datetime]:
    """Return the normalised event and its event time, or raise ``ValueError``."""

    if not isinstance(event, dict):
        raise ValueError("event must be a JSON object")
    if "event_type" not in event:
        raise ValueError("missing required field: event_type")
    event_type = _text(event, "event_type")
    if event_type not in TOPIC_SUFFIX:
        raise ValueError(f"unsupported event_type: {event_type!r}")
    missing = required_fields(event_type) - event.keys()
    if missing:
        raise ValueError("missing required fields: " + ", ".join(sorted(missing)))

    normalised: dict[str, Any] = {field: _text(event, field) for field in COMMON_TEXT_FIELDS}
    if any(char in normalised["station_id"] for char in "/+#"):
        raise ValueError("station_id cannot contain '/', '+' or '#' in an MQTT topic")
    timestamp = _timestamp(event, "event_time")

    if event_type != "price_update":
        normalised["session_id"] = _text(event, "session_id")
        connector = event["connector_id"]
        if isinstance(connector, bool) or not isinstance(connector, int) or connector < 1:
            raise ValueError("connector_id must be a positive integer")
        normalised["connector_id"] = connector
    for field in TIME_FIELDS.get(event_type, ()):
        _timestamp(event, field)
        normalised[field] = _text(event, field)
    for field, minimum in NUMERIC_FIELDS[event_type].items():
        normalised[field] = _number(event, field, minimum)
    for field in OPTIONAL_TEXT_FIELDS.get(event_type, ()):
        value = event.get(field)
        if value is not None and not isinstance(value, str):
            raise ValueError(f"{field} must be a string or null")
        normalised[field] = value.strip() if isinstance(value, str) and value.strip() else None
    return normalised, timestamp


def topic_for(prefix: str, event: dict[str, Any]) -> str:
    return f"{prefix.rstrip('/')}/{event['station_id']}/{TOPIC_SUFFIX[event['event_type']]}"


def describe(event: dict[str, Any]) -> str:
    """One-line log summary for publisher and subscriber output."""

    station = event["station_id"]
    if event["event_type"] == "price_update":
        detail = f"price={event['price_per_kwh']:.2f}"
    elif event["event_type"] == "meter":
        detail = (
            f"session={event['session_id']} kW={event['power_kw']:.1f} "
            f"kWh={event['energy_kwh']:.3f} soc={event['soc_pct']:.1f}"
        )
    elif event["event_type"] == "session_end":
        detail = f"session={event['session_id']} kWh={event['energy_kwh']:.2f} cost={event['cost_aud']:.2f}"
    else:
        detail = f"session={event['session_id']} connector={event['connector_id']}"
    return f"{event['event_type']} station={station} time={event['event_time']} {detail}"
