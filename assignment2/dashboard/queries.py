"""Read-side queries for the Assignment 2 dashboard (price stage).

Prices come only from events that C's storage has received
(``latest_station_price``). The generator's ``base_price`` and unreplayed CSV
prices are never treated as received prices.
"""

from __future__ import annotations

import csv
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb


def load_station_ids(path: str | Path) -> list[str]:
    """Station IDs from the generator's ``stations.csv``, in file order."""

    ids: list[str] = []
    with Path(path).open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None or "station_id" not in reader.fieldnames:
            raise ValueError(f"{path} must contain a station_id column")
        for row in reader:
            station_id = (row.get("station_id") or "").strip()
            if not station_id:
                raise ValueError(f"{path} contains a row without station_id")
            if station_id not in ids:
                ids.append(station_id)
    if not ids:
        raise ValueError(f"{path} contains no stations")
    return ids


def _text(value: Any) -> Any:
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


class DashboardData:
    """Query A1 station details and received prices from the A2 database.

    ``lock`` serialises database access with an in-process subscriber so a
    read-only connection never overlaps C's write connection.
    """

    def __init__(
        self,
        database_path: str | Path,
        station_ids: list[str],
        lock: threading.Lock | None = None,
    ) -> None:
        self.database_path = Path(database_path).resolve()
        if not self.database_path.is_file():
            raise FileNotFoundError(f"A2 database does not exist: {self.database_path}")
        self.station_ids = station_ids
        self.lock = lock or threading.Lock()

    def snapshot(self, recent_limit: int = 10) -> dict[str, Any]:
        """Station details, latest received prices and the latest price events."""

        with self.lock, duckdb.connect(str(self.database_path), read_only=True) as connection:
            rows = connection.execute(
                """
                SELECT
                    r.record_id, r.Station_name, r.Station_address, r.Operator,
                    r.Number_of_plugs, r.power_kw, r.Charger_Type, r.Latitude, r.Longitude,
                    c.plug_types,
                    p.event_id, p.event_ts, p.price_per_kwh, p.received_at,
                    date_diff(
                        'millisecond', p.received_at, CAST(current_timestamp AS TIMESTAMP)
                    ) / 1000.0 AS age_seconds
                FROM charger_records AS r
                LEFT JOIN latest_station_price AS p ON p.station_id = r.record_id
                -- Connector types from A1's Open Charge Map augmentation.
                LEFT JOIN (
                    SELECT record_id,
                           string_agg(DISTINCT connector_type, ', ' ORDER BY connector_type)
                               AS plug_types
                    FROM attribute_connectors
                    GROUP BY record_id
                ) AS c ON c.record_id = r.record_id
                WHERE r.record_id IN (SELECT unnest(?))
                """,
                [self.station_ids],
            ).fetchall()
            recent = connection.execute(
                """
                SELECT station_id, event_ts, price_per_kwh
                FROM price_events
                WHERE station_id IN (SELECT unnest(?))
                ORDER BY received_at DESC, event_ts DESC, event_id DESC
                LIMIT ?
                """,
                [self.station_ids, recent_limit],
            ).fetchall()
            stored, replay_time = connection.execute(
                """
                SELECT count(*), max(event_ts)
                FROM price_events
                WHERE station_id IN (SELECT unnest(?))
                """,
                [self.station_ids],
            ).fetchone()

        by_id = {row[0]: row for row in rows}
        stations = []
        for station_id in self.station_ids:
            row = by_id.get(station_id)
            if row is None:
                # No A1 record means no A1 coordinates; reported, not guessed.
                continue
            latest = None
            if row[10] is not None:
                latest = {
                    "event_id": row[10],
                    "event_time": _iso(row[11]),
                    "price_per_kwh": float(row[12]),
                    "received_at": _iso(row[13]),
                    "age_seconds": None if row[14] is None else float(row[14]),
                }
            stations.append(
                {
                    "station_id": station_id,
                    "name": _text(row[1]),
                    "address": _text(row[2]),
                    "operator": _text(row[3]),
                    "plugs": row[4],
                    "power_kw": None if row[5] is None else float(row[5]),
                    "charger_type": _text(row[6]),
                    "lat": row[7],
                    "lon": row[8],
                    "plug_types": _text(row[9]),
                    "latest": latest,
                }
            )
        return {
            "stations": stations,
            "missing_station_ids": [s for s in self.station_ids if s not in by_id],
            "recent_events": [
                {"station_id": r[0], "event_time": _iso(r[1]), "price_per_kwh": float(r[2])}
                for r in recent
            ],
            "stored_events": stored,
            "replay_time": _iso(replay_time),
            "queried_at": datetime.now().isoformat(timespec="seconds"),
        }
