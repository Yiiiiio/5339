"""Read-side queries for the Assignment 2 dashboard.

Everything shown comes from events that the storage layer has received:
``latest_station_price``, ``station_session_metrics`` and the session/meter
tables. The generator's ``base_price`` and the generated CSVs are never read,
so the page cannot show a price or session state before its event arrives.
"""

from __future__ import annotations

import csv
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from assignment2.storage.price_store import get_price_history
from assignment2.storage.session_store import get_session_readings, get_station_sessions


@dataclass(frozen=True)
class SimulatedStation:
    """One row of the generator's ``stations.csv``."""

    station_id: str
    label: str | None
    site_type: str | None
    connectors: int | None
    max_kw: float | None


def _blank_to_none(value: Any) -> Any:
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _optional_int(value: str | None) -> int | None:
    value = _blank_to_none(value)
    return None if value is None else int(float(value))


def _optional_float(value: str | None) -> float | None:
    value = _blank_to_none(value)
    return None if value is None else float(value)


def load_simulated_stations(path: str | Path) -> list[SimulatedStation]:
    """Read the stations that the generator simulated, keeping file order."""

    stations: dict[str, SimulatedStation] = {}
    with Path(path).open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None or "station_id" not in reader.fieldnames:
            raise ValueError(f"{path} must contain a station_id column")
        for row in reader:
            station_id = (row.get("station_id") or "").strip()
            if not station_id:
                raise ValueError(f"{path} contains a row without station_id")
            if station_id in stations:
                continue
            stations[station_id] = SimulatedStation(
                station_id=station_id,
                label=_blank_to_none(row.get("name")),
                site_type=_blank_to_none(row.get("site_type")),
                connectors=_optional_int(row.get("connectors")),
                max_kw=_optional_float(row.get("max_kw")),
            )
    if not stations:
        raise ValueError(f"{path} contains no stations")
    return list(stations.values())


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _float(value: Any) -> float | None:
    return None if value is None else float(value)


class DashboardData:
    """Query station details and received prices from the A2 database.

    ``lock`` serialises database access with an in-process subscriber so a
    read-only connection never overlaps C's write connection.
    """

    def __init__(
        self,
        database_path: str | Path,
        stations: list[SimulatedStation],
        lock: threading.Lock | None = None,
    ) -> None:
        self.database_path = Path(database_path).resolve()
        if not self.database_path.is_file():
            raise FileNotFoundError(f"A2 database does not exist: {self.database_path}")
        self.stations = stations
        self.station_ids = [station.station_id for station in stations]
        self.lock = lock or threading.Lock()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.database_path), read_only=True)

    def snapshot(self, recent_limit: int = 10) -> dict[str, Any]:
        """Return station details, latest prices and the most recent events."""

        with self.lock, self._connect() as connection:
            detail_rows = connection.execute(
                """
                SELECT
                    r.record_id,
                    r.Station_name,
                    r.Station_address,
                    r.Operator,
                    r.Number_of_plugs,
                    r.power_kw,
                    r.Charger_rating,
                    r.charger_type_standardized,
                    r.Latitude,
                    r.Longitude,
                    p.event_id,
                    p.event_ts,
                    p.price_per_kwh,
                    p.wholesale_per_kwh,
                    p.received_at,
                    date_diff(
                        'millisecond', p.received_at, CAST(current_timestamp AS TIMESTAMP)
                    ) / 1000.0 AS price_age_seconds,
                    coalesce(m.active_sessions, 0),
                    coalesce(m.occupied_connectors, 0),
                    coalesce(m.completed_sessions, 0),
                    coalesce(m.completed_energy_kwh, 0),
                    date_diff(
                        'millisecond', s.last_received_at, CAST(current_timestamp AS TIMESTAMP)
                    ) / 1000.0 AS session_age_seconds,
                    c.plug_types,
                    p.event_id IS NOT NULL
                        OR m.station_id IS NOT NULL
                        OR EXISTS (
                            SELECT 1 FROM meter_readings AS mr WHERE mr.station_id = r.record_id
                        ) AS has_events
                FROM charger_records AS r
                LEFT JOIN latest_station_price AS p
                    ON p.station_id = r.record_id
                LEFT JOIN station_session_metrics AS m
                    ON m.station_id = r.record_id
                LEFT JOIN (
                    SELECT station_id, max(received_at) AS last_received_at
                    FROM session_events
                    GROUP BY station_id
                ) AS s
                    ON s.station_id = r.record_id
                -- Connector types from A1's Open Charge Map augmentation.
                LEFT JOIN (
                    SELECT record_id,
                           string_agg(DISTINCT connector_type, ', ' ORDER BY connector_type)
                               AS plug_types
                    FROM attribute_connectors
                    GROUP BY record_id
                ) AS c
                    ON c.record_id = r.record_id
                WHERE r.record_id IN (SELECT unnest(?))
                """,
                [self.station_ids],
            ).fetchall()
            # Meter readings are left out: they would crowd out every other event.
            recent_rows = connection.execute(
                """
                SELECT event_id, event_type, station_id, event_ts, detail, received_at
                FROM (
                    SELECT event_id, event_type, station_id, event_ts,
                           price_per_kwh AS detail, received_at
                    FROM price_events
                    UNION ALL
                    SELECT event_id, event_type, station_id, event_ts,
                           CAST(connector_id AS DECIMAL(12, 4)) AS detail, received_at
                    FROM session_events
                )
                WHERE station_id IN (SELECT unnest(?))
                ORDER BY received_at DESC, event_ts DESC, event_id DESC
                LIMIT ?
                """,
                [self.station_ids, recent_limit],
            ).fetchall()
            counts = connection.execute(
                """
                SELECT
                    (SELECT count(*) FROM price_events WHERE station_id IN (SELECT unnest($1))),
                    (SELECT count(*) FROM session_events
                     WHERE station_id IN (SELECT unnest($1)) AND event_type = 'session_start'),
                    (SELECT count(*) FROM session_events
                     WHERE station_id IN (SELECT unnest($1)) AND event_type = 'session_end'),
                    (SELECT count(*) FROM meter_readings WHERE station_id IN (SELECT unnest($1))),
                    (SELECT max(event_ts) FROM (
                        SELECT event_ts FROM price_events WHERE station_id IN (SELECT unnest($1))
                        UNION ALL
                        SELECT event_ts FROM session_events WHERE station_id IN (SELECT unnest($1))
                        UNION ALL
                        SELECT event_ts FROM meter_readings WHERE station_id IN (SELECT unnest($1))
                    ))
                """,
                [self.station_ids],
            ).fetchone()

        details = {row[0]: row for row in detail_rows}
        stations = []
        missing_station_ids = []
        for simulated in self.stations:
            row = details.get(simulated.station_id)
            if row is None:
                # No A1 record means no A1 coordinates; report instead of guessing.
                missing_station_ids.append(simulated.station_id)
                continue
            latest = None
            if row[10] is not None:
                latest = {
                    "event_id": row[10],
                    "event_time": _iso(row[11]),
                    "price_per_kwh": _float(row[12]),
                    "wholesale_per_kwh": _float(row[13]),
                    "received_at": _iso(row[14]),
                }
            ages = [age for age in (row[15], row[20]) if age is not None]
            stations.append(
                {
                    "station_id": simulated.station_id,
                    "name": _blank_to_none(row[1]),
                    "generator_label": simulated.label,
                    "address": _blank_to_none(row[2]),
                    "operator": _blank_to_none(row[3]),
                    "plugs": row[4],
                    "power_kw": _float(row[5]),
                    "charger_rating": _blank_to_none(row[6]),
                    "charger_type": _blank_to_none(row[7]),
                    "plug_types": _blank_to_none(row[21]),
                    "lat": row[8],
                    "lon": row[9],
                    "site_type": simulated.site_type,
                    "simulated_connectors": simulated.connectors,
                    "latest": latest,
                    "active_sessions": row[16],
                    "occupied_connectors": row[17],
                    "completed_sessions": row[18],
                    "completed_energy_kwh": float(row[19]),
                    # Seconds since the last price or session event arrived (meters excluded).
                    "activity_age_seconds": min(ages) if ages else None,
                    # A marker appears once any message for the station has arrived.
                    "has_events": bool(row[22]),
                }
            )

        return {
            "stations": stations,
            "missing_station_ids": missing_station_ids,
            "recent_events": [
                {
                    "event_id": row[0],
                    "event_type": row[1],
                    "station_id": row[2],
                    "event_time": _iso(row[3]),
                    # Price for price_update, connector number for session events.
                    "detail": _float(row[4]),
                    "received_at": _iso(row[5]),
                }
                for row in recent_rows
            ],
            "stored_events": {
                "price_update": counts[0],
                "session_start": counts[1],
                "session_end": counts[2],
                "meter": counts[3],
            },
            # Latest event time received: the replay's position in simulated time.
            "replay_time": _iso(counts[4]),
            "queried_at": datetime.now().isoformat(timespec="seconds"),
        }

    def price_history(self, station_id: str) -> list[dict[str, Any]]:
        """Return the received price history for one station."""

        with self.lock:
            return get_price_history(self.database_path, station_id)

    def station_sessions(self, station_id: str) -> list[dict[str, Any]]:
        """Active sessions (latest start first), then recent completed ones."""

        with self.lock:
            return get_station_sessions(self.database_path, station_id)

    def session_readings(self, session_id: str) -> list[dict[str, Any]]:
        """Received meter readings of one session in event-time order."""

        with self.lock:
            return get_session_readings(self.database_path, session_id)
