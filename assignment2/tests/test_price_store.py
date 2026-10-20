from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

import duckdb

from assignment2.storage.price_store import (
    get_latest_price,
    get_price_history,
    initialise_database,
    store_price_event,
)


ROOT = Path(__file__).resolve().parents[2]
STATIONS_CSV = ROOT / "assignment2/data/generated/sample_20/stations.csv"
PRICES_CSV = ROOT / "assignment2/data/generated/sample_20/prices_hourly.csv"


class PriceStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_dir.name) / "test.duckdb"

        with STATIONS_CSV.open(encoding="utf-8-sig", newline="") as handle:
            self.station_rows = list(csv.DictReader(handle))

        with duckdb.connect(str(self.database_path)) as connection:
            connection.execute(
                "CREATE TABLE charger_records (record_id VARCHAR PRIMARY KEY)"
            )
            connection.executemany(
                "INSERT INTO charger_records VALUES (?)",
                [(row["station_id"],) for row in self.station_rows],
            )
        initialise_database(self.database_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @staticmethod
    def _event(row: dict[str, str], suffix: str) -> dict[str, object]:
        return {
            "event_id": f"price_update:{row['station_id']}:{row['ts']}:{suffix}",
            "event_type": "price_update",
            "station_id": row["station_id"],
            "event_time": row["ts"],
            "price_per_kwh": row["price_per_kwh"],
            "wholesale_per_kwh": row["wholesale_per_kwh"],
        }

    def test_real_sample_insert_duplicate_history_and_latest(self) -> None:
        with PRICES_CSV.open(encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))

        station_id = rows[0]["station_id"]
        station_rows = [row for row in rows if row["station_id"] == station_id]
        first = station_rows[0]
        later = next(
            row
            for row in station_rows[1:]
            if row["price_per_kwh"] != first["price_per_kwh"]
        )

        first_event = self._event(first, "first")
        later_event = self._event(later, "later")

        self.assertEqual(
            store_price_event(self.database_path, first_event), "inserted"
        )
        self.assertEqual(
            store_price_event(self.database_path, first_event), "duplicate"
        )
        self.assertEqual(
            store_price_event(self.database_path, later_event), "inserted"
        )

        history = get_price_history(self.database_path, station_id)
        latest = get_latest_price(self.database_path, station_id)
        self.assertEqual(len(history), 2)
        self.assertEqual(latest["event_id"], later_event["event_id"])
        self.assertEqual(latest["price_per_kwh"], float(later["price_per_kwh"]))

    def test_unknown_station_is_rejected(self) -> None:
        event = {
            "event_id": "price_update:missing:2026-09-15T00:00:00",
            "event_type": "price_update",
            "station_id": "missing",
            "event_time": "2026-09-15T00:00:00",
            "price_per_kwh": 0.7,
            "wholesale_per_kwh": 0.07,
        }
        with self.assertRaisesRegex(ValueError, "Unknown station_id"):
            store_price_event(self.database_path, event)

    def test_same_station_and_time_with_different_price_is_rejected(self) -> None:
        station_id = self.station_rows[0]["station_id"]
        first = {
            "event_id": "first",
            "event_type": "price_update",
            "station_id": station_id,
            "event_time": "2026-09-15T00:00:00",
            "price_per_kwh": 0.7,
            "wholesale_per_kwh": 0.07,
        }
        conflict = dict(first, event_id="conflict", price_per_kwh=0.8)
        store_price_event(self.database_path, first)
        with self.assertRaisesRegex(ValueError, "Conflicting price values"):
            store_price_event(self.database_path, conflict)

    def test_late_old_event_does_not_replace_latest(self) -> None:
        station_id = self.station_rows[0]["station_id"]
        newer = {
            "event_id": "newer",
            "event_type": "price_update",
            "station_id": station_id,
            "event_time": "2026-09-15T03:00:00",
            "price_per_kwh": 0.8,
            "wholesale_per_kwh": 0.08,
        }
        older = dict(
            newer,
            event_id="older",
            event_time="2026-09-15T02:00:00",
            price_per_kwh=0.6,
        )
        store_price_event(self.database_path, newer)
        store_price_event(self.database_path, older)
        self.assertEqual(
            get_latest_price(self.database_path, station_id)["event_id"], "newer"
        )


if __name__ == "__main__":
    unittest.main()
