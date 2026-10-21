"""Check the price-stage dashboard reads only received prices from C's database.

No broker or browser is used. B's subscriber callback receives the real
replay events and writes to a cleared copy of the submitted A2 database.
"""

import io
import json
import tempfile
import threading
import unittest
from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from assignment2.dashboard.app import (
    NO_PRICE_TEXT,
    build_markers,
    create_app,
    marker_label,
    start_subscriber,
)
from assignment2.dashboard.prepare_demo_db import prepare_demo_database
from assignment2.dashboard.queries import DashboardData, load_station_ids
from assignment2.storage.price_store import get_latest_price

ROOT = Path(__file__).resolve().parents[2]
A2_DB = ROOT / "assignment2/data/final/project_a2.duckdb"
STATIONS = ROOT / "assignment2/data/generated/sample_20/stations.csv"
EVENTS = ROOT / "assignment2/data/events/price_events.jsonl"


def deliver(subscriber, event):
    message = SimpleNamespace(
        payload=json.dumps(event).encode(),
        topic=f"evchargers/{event['station_id']}/price",
    )
    subscriber.on_message(None, None, message)


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.db = Path(self.folder.name) / "demo.duckdb"
        prepare_demo_database(A2_DB, self.db)
        self.lock = threading.Lock()
        self.data = DashboardData(self.db, load_station_ids(STATIONS), self.lock)
        self.events = [json.loads(l) for l in EVENTS.read_text(encoding="utf-8").splitlines() if l.strip()]

    def tearDown(self):
        self.folder.cleanup()

    def start_subscriber(self):
        args = Namespace(
            mqtt_host="localhost", mqtt_port=1883, mqtt_topic="evchargers/+/price",
            mqtt_client_id="dashboard-test", username=None, password=None,
            received_log=None, database=self.db,
        )
        with patch("assignment2.mqtt.subscriber.mqtt.Client"), \
                patch("assignment2.dashboard.app.threading.Thread"):
            return start_subscriber(args, self.lock)

    def replay(self, subscriber, events):
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            for event in events:
                deliver(subscriber, event)

    def test_cleared_copy_shows_all_stations_without_price(self):
        snapshot = self.data.snapshot()
        self.assertEqual(len(snapshot["stations"]), 20)
        self.assertEqual(snapshot["missing_station_ids"], [])
        self.assertEqual(snapshot["stored_events"], 0)
        labels = [m.children[0].children.children for m in build_markers(snapshot, 5)]
        self.assertEqual(labels, [NO_PRICE_TEXT] * 20)
        self.assertEqual(sum(s["plug_types"] is not None for s in snapshot["stations"]), 9)
        with self.assertRaises(ValueError):
            prepare_demo_database(A2_DB, A2_DB)
        with self.assertRaises(FileExistsError):
            prepare_demo_database(A2_DB, self.db)

    def test_received_prices_drive_the_map(self):
        subscriber = self.start_subscriber()
        self.replay(subscriber, [e for e in self.events if e["event_time"] == "2026-09-15T15:00:00"])
        snapshot = self.data.snapshot()
        for station in snapshot["stations"]:
            latest = get_latest_price(self.db, station["station_id"])
            self.assertEqual(station["latest"]["event_id"], latest["event_id"])
            self.assertEqual(marker_label(station), f"${latest['price_per_kwh']:.2f}")
            self.assertEqual(station["latest"]["event_time"], "2026-09-15T15:00:00")

        self.replay(subscriber, self.events)
        snapshot = self.data.snapshot()
        self.assertEqual(snapshot["stored_events"], 60)
        self.assertEqual({s["latest"]["event_time"] for s in snapshot["stations"]}, {"2026-09-15T17:00:00"})
        self.assertEqual(subscriber.storage_error_count, 0)
        self.assertEqual(subscriber.duplicate_count, 20)

    def test_concurrent_reads_do_not_break_writes(self):
        subscriber = self.start_subscriber()
        errors = []
        writer = threading.Thread(target=self.replay, args=(subscriber, self.events))
        writer.start()
        while writer.is_alive():
            try:
                self.data.snapshot()
            except Exception as exc:  # any read failure is a test failure
                errors.append(exc)
        writer.join()
        self.assertEqual(errors, [])
        self.assertEqual(subscriber.storage_error_count, 0)
        self.assertEqual(subscriber.accepted_count, 60)

    def test_app_layout_builds(self):
        self.assertIsNotNone(create_app(self.data).layout)


if __name__ == "__main__":
    unittest.main()
