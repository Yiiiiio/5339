"""Check the dashboard reads only received events from the A2 database.

No broker or browser is used. B's subscriber callback receives the real
four-type replay and writes to a cleared copy of the submitted A2 database.
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
    build_markers,
    charging_figure,
    create_app,
    marker_label,
    popup_curve,
    start_subscriber,
    station_signature,
)
from assignment2.dashboard.prepare_demo_db import prepare_demo_database
from assignment2.dashboard.queries import DashboardData, load_simulated_stations
from assignment2.mqtt.event_schema import topic_for

ROOT = Path(__file__).resolve().parents[2]
A2_DB = ROOT / "assignment2/data/final/project_a2.duckdb"
STATIONS = ROOT / "assignment2/data/generated/sample_20/stations.csv"
EVENTS = ROOT / "assignment2/data/events/replay_events.jsonl"


def load_events():
    return [
        json.loads(line)
        for line in EVENTS.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def deliver(subscriber, event):
    message = SimpleNamespace(
        payload=json.dumps(event).encode(),
        topic=topic_for("evchargers", event),
    )
    subscriber.on_message(None, None, message)


def expected_state(events):
    """Active/completed sessions per station from the events themselves."""
    started, ended = {}, set()
    for event in events:
        if event["event_type"] == "session_start":
            started[event["session_id"]] = event["station_id"]
        elif event["event_type"] == "session_end":
            ended.add(event["session_id"])
    state = {}
    for session_id, station_id in started.items():
        active, completed = state.get(station_id, (0, 0))
        state[station_id] = (active, completed + 1) if session_id in ended else (active + 1, completed)
    return state


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.db = Path(self.folder.name) / "demo.duckdb"
        prepare_demo_database(A2_DB, self.db)
        self.stations = load_simulated_stations(STATIONS)
        self.lock = threading.Lock()
        self.data = DashboardData(self.db, self.stations, self.lock)
        self.events = load_events()

    def tearDown(self):
        self.folder.cleanup()

    def start_subscriber(self):
        args = Namespace(
            mqtt_host="localhost", mqtt_port=1883, mqtt_topic="evchargers/+/+",
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

    def test_cleared_copy_shows_empty_states(self):
        snapshot = self.data.snapshot()
        self.assertEqual(len(snapshot["stations"]), len(self.stations))
        self.assertEqual(snapshot["missing_station_ids"], [])
        self.assertEqual(set(snapshot["stored_events"].values()), {0})
        self.assertIsNone(snapshot["replay_time"])
        for station in snapshot["stations"]:
            self.assertIsNone(station["latest"])
            self.assertEqual(station["active_sessions"], 0)
            self.assertFalse(station["has_events"])
        # No message yet, so no marker on the map.
        self.assertEqual(build_markers(snapshot, "price", 5), [])
        # Plug types come from A1's Open Charge Map augmentation (9 of 20 matched).
        self.assertEqual(sum(s["plug_types"] is not None for s in snapshot["stations"]), 9)

        with self.assertRaises(ValueError):
            prepare_demo_database(A2_DB, A2_DB)
        with self.assertRaises(FileExistsError):
            prepare_demo_database(A2_DB, self.db)

    def test_partial_replay_matches_events(self):
        subscriber = self.start_subscriber()
        cutoff = "2026-09-15T16:30:00"
        received = [e for e in self.events if e["event_time"] < cutoff]
        self.replay(subscriber, received)
        self.assertEqual(subscriber.storage_error_count, 0)

        snapshot = self.data.snapshot()
        self.assertLess(snapshot["replay_time"], cutoff)
        expected = expected_state(received)
        for station in snapshot["stations"]:
            active, completed = expected.get(station["station_id"], (0, 0))
            self.assertEqual(station["active_sessions"], active, station["station_id"])
            self.assertEqual(station["completed_sessions"], completed, station["station_id"])
            self.assertEqual(station["latest"]["event_time"], "2026-09-15T16:00:00")
        self.assertGreater(sum(s["active_sessions"] for s in snapshot["stations"]), 0)

        # Markers exist only for stations with a received message; labels show the price.
        markers = build_markers(snapshot, "price", 5)
        self.assertEqual(len(markers), sum(s["has_events"] for s in snapshot["stations"]))
        for marker in markers:
            station = next(s for s in snapshot["stations"] if s["station_id"] == marker.id["id"])
            label = marker.children[0].children.children
            self.assertEqual(label, f"${station['latest']['price_per_kwh']:.2f}")
            self.assertEqual(label, marker_label(station, "price"))

        # Selected station: sessions listed active first; readings form a curve.
        busy = next(s for s in snapshot["stations"] if s["active_sessions"])
        sessions = self.data.station_sessions(busy["station_id"])
        self.assertEqual(sessions[0]["status"], "active")
        readings = self.data.session_readings(sessions[0]["session_id"])
        self.assertEqual(readings, sorted(readings, key=lambda r: r["event_time"]))
        self.assertTrue(all(r["event_time"] < cutoff for r in readings))
        figure = charging_figure(readings, sessions[0]["session_id"])
        self.assertEqual(len(figure.data), 3)
        caption, popup_figure, style = popup_curve(self.data, busy)
        self.assertIn("Live charging session", caption)
        self.assertEqual(style["display"], "block")
        self.assertEqual(list(popup_figure.data[0].y), [r["energy_kwh"] for r in readings])
        idle = next(s for s in snapshot["stations"] if s["active_sessions"] == 0)
        self.assertEqual(popup_curve(self.data, idle)[2], {"display": "none"})

        # Replaying everything again changes nothing (idempotent storage).
        before = [station_signature(s, "occupancy", 0) for s in snapshot["stations"]]
        self.replay(subscriber, received)
        after = [station_signature(s, "occupancy", 0) for s in self.data.snapshot()["stations"]]
        self.assertEqual(before, after)
        self.assertEqual(subscriber.duplicate_count, len(received))

    def test_concurrent_reads_during_replay(self):
        subscriber = self.start_subscriber()
        received = [e for e in self.events if e["event_time"] < "2026-09-15T15:30:00"]
        errors = []
        writer = threading.Thread(target=self.replay, args=(subscriber, received))
        writer.start()
        while writer.is_alive():
            try:
                self.data.snapshot()
            except Exception as exc:  # any read failure is a test failure
                errors.append(exc)
        writer.join()

        self.assertEqual(errors, [])
        self.assertEqual(subscriber.storage_error_count, 0)
        self.assertEqual(subscriber.accepted_count, len(received))
        snapshot = self.data.snapshot()
        self.assertEqual(
            snapshot["stored_events"]["meter"],
            sum(e["event_type"] == "meter" for e in received),
        )
        expected = expected_state(received)
        for station in snapshot["stations"]:
            self.assertEqual(
                (station["active_sessions"], station["completed_sessions"]),
                expected.get(station["station_id"], (0, 0)),
            )

    def test_app_layout_builds(self):
        app = create_app(self.data, refresh_seconds=1, highlight_seconds=5)
        self.assertIsNotNone(app.layout)


if __name__ == "__main__":
    unittest.main()
