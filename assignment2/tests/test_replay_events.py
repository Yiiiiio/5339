"""Check the four-type replay file and the publisher/subscriber schema."""

import json
import csv
import unittest
from datetime import datetime
from pathlib import Path

from assignment2.mqtt import publisher, subscriber
from assignment2.mqtt.event_schema import EVENT_ORDER, topic_for
from assignment2.mqtt.prepare_replay_events import prepare_replay

ROOT = Path(__file__).resolve().parents[2]
GENERATED = ROOT / "assignment2/data/generated/dc_data"
EVENTS = ROOT / "assignment2/data/events"
START = datetime(2026, 9, 15, 0, 55)
END = datetime(2026, 9, 15, 1, 5)


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


class ReplayEventTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.generated_events = prepare_replay(
            GENERATED / "prices_hourly.csv",
            GENERATED / "sessions.csv",
            GENERATED / "session_metering.csv",
            START,
            END,
        )

        cls.events = read_jsonl(EVENTS / "replay_events.jsonl")

    def test_generated_events_cover_input_stations(self):
        with (GENERATED / "stations.csv").open(newline="") as stream:
            station_ids = {row["station_id"] for row in csv.DictReader(stream)}
        prices = [e for e in self.generated_events if e["event_type"] == "price_update"]
        self.assertEqual({e["station_id"] for e in prices}, station_ids)
        self.assertEqual(self.events, self.generated_events)
        for event in self.generated_events:
            received = subscriber.validate_event(json.dumps(event).encode(), topic_for("evchargers", event))
            self.assertEqual(received["event_id"], event["event_id"])
        keys = [(e["event_time"], EVENT_ORDER[e["event_type"]]) for e in self.generated_events]
        self.assertEqual(keys, sorted(keys))

    def test_counts_and_price_events_match_price_stage(self):
        counts = {event_type: 0 for event_type in EVENT_ORDER}
        for event in self.events:
            counts[event["event_type"]] += 1
        self.assertEqual(
            counts, {'price_update': 866, 'session_start': 25, 'meter': 585, 'session_end': 6}
        )
        prices = [e for e in self.events if e["event_type"] == "price_update"]
        self.assertEqual(prices, read_jsonl(EVENTS / "price_events.jsonl"))

    def test_order_and_window_boundaries(self):
        keys = [(e["event_time"], EVENT_ORDER[e["event_type"]]) for e in self.events]
        self.assertEqual(keys, sorted(keys))
        self.assertTrue(all(e["event_time"] < END.isoformat() for e in self.events))

        position = {}
        for index, event in enumerate(self.events):
            if "session_id" in event:
                position.setdefault(event["session_id"], []).append((index, event["event_type"]))
        for session_id, items in position.items():
            types = [event_type for _, event_type in items]
            self.assertEqual(types[0], "session_start", session_id)
            if "session_end" in types:
                self.assertEqual(types[-1], "session_end", session_id)
                self.assertEqual(types.count("session_end"), 1)

        # Sessions already charging at the window start keep their start event.
        early = [e for e in self.events
                 if e["event_type"] == "session_start" and e["event_time"] < START.isoformat()]
        self.assertEqual(len(early), 20)

    def test_start_carries_no_final_results(self):
        for event in self.events:
            if event["event_type"] == "session_start":
                self.assertFalse({"energy_kwh", "cost_aud", "charge_end_time"} & event.keys())

    def test_publisher_and_subscriber_accept_all_types(self):
        loaded = publisher.load_events(EVENTS / "replay_events.jsonl")
        self.assertEqual(len(loaded), len(self.events))
        for event in self.events[:200]:
            topic = topic_for("evchargers", event)
            received = subscriber.validate_event(json.dumps(event).encode(), topic)
            self.assertEqual(received["event_id"], event["event_id"])

    def test_subscriber_rejects_topic_type_mismatch(self):
        meter = next(e for e in self.events if e["event_type"] == "meter")
        wrong_topic = f"evchargers/{meter['station_id']}/price"
        with self.assertRaises(ValueError):
            subscriber.validate_event(json.dumps(meter).encode(), wrong_topic)


if __name__ == "__main__":
    unittest.main()
