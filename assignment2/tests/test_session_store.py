"""Check session and meter storage against a temporary copy of the A1 database."""

import json
import shutil
import tempfile
import unittest
from pathlib import Path

import duckdb

from assignment2.storage.price_store import initialise_database
from assignment2.storage.session_store import (
    get_session_readings,
    get_station_session_metrics,
    get_station_sessions,
    store_event,
)

ROOT = Path(__file__).resolve().parents[2]
EVENTS = ROOT / "assignment2/data/events/replay_events.jsonl"


def load_events():
    return [json.loads(line) for line in EVENTS.read_text(encoding="utf-8").splitlines() if line.strip()]


class SessionStoreTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.db = Path(self.folder.name) / "sessions.duckdb"
        shutil.copy2(ROOT / "data/final/project.duckdb", self.db)
        initialise_database(self.db)
        events = load_events()
        self.session_id = next(e["session_id"] for e in events if e["event_type"] == "session_end")
        self.session = [e for e in events if e.get("session_id") == self.session_id]
        self.station_id = self.session[0]["station_id"]

    def tearDown(self):
        self.folder.cleanup()

    def test_lifecycle_duplicates_and_metrics(self):
        start, *meters, end = self.session
        self.assertEqual(store_event(self.db, start), "inserted")
        metrics = get_station_session_metrics(self.db, self.station_id)
        self.assertEqual((metrics["active_sessions"], metrics["completed_sessions"]), (1, 0))

        for meter in meters:
            self.assertEqual(store_event(self.db, meter), "inserted")
        self.assertEqual(store_event(self.db, meters[0]), "duplicate")
        self.assertEqual(store_event(self.db, start), "duplicate")
        readings = get_session_readings(self.db, self.session_id)
        self.assertEqual([r["event_time"] for r in readings], [m["event_time"] for m in meters])

        self.assertEqual(store_event(self.db, end), "inserted")
        self.assertEqual(store_event(self.db, end), "duplicate")
        metrics = get_station_session_metrics(self.db, self.station_id)
        self.assertEqual((metrics["active_sessions"], metrics["completed_sessions"]), (0, 1))
        self.assertAlmostEqual(metrics["completed_energy_kwh"], end["energy_kwh"])
        # Cumulative meter energy is not summed: the session total is the end value.
        self.assertLess(metrics["completed_energy_kwh"], sum(m["energy_kwh"] for m in meters))
        sessions = get_station_sessions(self.db, self.station_id)
        self.assertEqual(sessions[0]["status"], "completed")
        self.assertEqual(sessions[0]["readings"], len(meters))

    def test_end_before_start_counts_one_session(self):
        start, *_, end = self.session
        store_event(self.db, end)
        store_event(self.db, start)
        metrics = get_station_session_metrics(self.db, self.station_id)
        self.assertEqual((metrics["active_sessions"], metrics["completed_sessions"]), (0, 1))

    def test_conflicts_are_rejected(self):
        start, first_meter, *_ = self.session
        store_event(self.db, start)
        store_event(self.db, first_meter)
        with self.assertRaises(ValueError):
            store_event(self.db, dict(start, price_per_kwh=9.0))
        with self.assertRaises(ValueError):
            store_event(self.db, dict(start, event_id="other-id", vehicle_model="Other"))
        with self.assertRaises(ValueError):
            store_event(self.db, dict(first_meter, event_id="moved", event_time="2026-09-15T23:59:00",
                                      connector_id=first_meter["connector_id"] + 1))
        with self.assertRaises(ValueError):
            store_event(self.db, dict(start, event_id="nowhere", station_id="not-a-station",
                                      session_id="nowhere"))
        with self.assertRaises(ValueError):
            store_event(self.db, dict(first_meter, event_id="bad-soc", event_time="2026-09-15T23:58:00",
                                      soc_pct=120.0))
        with duckdb.connect(str(self.db), read_only=True) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM session_events").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT count(*) FROM meter_readings").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
