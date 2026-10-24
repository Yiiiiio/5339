"""Exercise B's real messages through the subscriber callback into C's database.

No broker is contacted. A temporary copy of the actual A1 database is used.
"""

import io
import json
import shutil
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import duckdb

from assignment2.mqtt.subscriber import PriceSubscriber
from assignment2.storage.price_store import get_latest_price, get_price_history

ROOT = Path(__file__).resolve().parents[2]


class PriceIntegrationTests(unittest.TestCase):
    def test_real_events_restart_conflict_and_retry(self):
        events = [
            json.loads(line)
            for line in (ROOT / "assignment2/data/events/price_events.jsonl")
            .read_text(encoding="utf-8").splitlines() if line.strip()
        ]
        station_ids = list(dict.fromkeys(e["station_id"] for e in events))[:8]
        events = [e for e in events if e["station_id"] in station_ids]
        event_count = len(events)
        self.assertEqual(event_count, 2 * len(station_ids))
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / "integration.duckdb"
            shutil.copy2(ROOT / "data/final/project.duckdb", db)
            args = Namespace(
                database=db, username=None, password=None, client_id="offline-test",
                output=None, max_messages=event_count,
            )

            def deliver(subscriber, event):
                message = SimpleNamespace(
                    payload=json.dumps(event).encode(),
                    topic=f"evchargers/{event['station_id']}/price",
                )
                subscriber.on_message(None, None, message)

            with patch("assignment2.mqtt.subscriber.mqtt.Client"), \
                    redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                first = PriceSubscriber(args)
                for event in events:
                    deliver(first, event)
                self.assertEqual(first.accepted_count, event_count)
                self.assertEqual(first.storage_error_count, 0)
                self.assertTrue(first.stop_event.is_set())

                restarted = PriceSubscriber(args)
                for event in events:
                    deliver(restarted, event)
                self.assertEqual(restarted.accepted_count, 0)
                self.assertEqual(restarted.duplicate_count, event_count)
                self.assertTrue(restarted.stop_event.is_set())

                # A reused ID with altered content must be checked by the DB,
                # even if that ID was already seen during this process.
                deliver(restarted, dict(events[0], price_per_kwh=9.0))
                self.assertEqual(restarted.storage_error_count, 1)

                retry = PriceSubscriber(args)
                new_event = dict(events[0], event_id="retry-example",
                                 event_time="2026-09-15T18:00:00")
                with patch.object(retry, "store_event", side_effect=OSError("busy")):
                    deliver(retry, new_event)
                self.assertNotIn("retry-example", retry.seen_event_ids)
                deliver(retry, new_event)
                self.assertEqual(retry.accepted_count, 1)

            # Remove the deliberately added retry event in this temporary DB.
            with duckdb.connect(str(db)) as con:
                con.execute("DELETE FROM price_events WHERE event_id = 'retry-example'")
                self.assertEqual(con.execute("SELECT count(*) FROM price_events").fetchone()[0], event_count)
                self.assertEqual(con.execute("SELECT count(*) FROM latest_station_price").fetchone()[0], len(station_ids))
            expected = {}
            for event in events:
                expected[event["station_id"]] = event
            for station_id, event in expected.items():
                latest = get_latest_price(db, station_id)
                self.assertEqual(latest["event_time"], "2026-09-15T01:00:00")
                self.assertEqual(latest["price_per_kwh"], event["price_per_kwh"])
                self.assertEqual(len(get_price_history(db, station_id)), 2)
                self.assertNotIn("ts", latest)


if __name__ == "__main__":
    unittest.main()
