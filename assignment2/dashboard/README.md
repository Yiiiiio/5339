# Dashboard (role D, price stage)

Thursday check-in version. A Dash + dash-leaflet map of the 20 sample
stations at their A1 coordinates. Each marker carries a label with the latest
**received** price from C's `latest_station_price` view, or "No price yet";
the page polls the database every second (`--refresh-seconds`). Clicking a
marker opens a popup with name, address, operator, plug types (from A1's
Open Charge Map augmentation; 9 of 20 matched), plugs, power and the latest
price. Missing A1 fields show "Missing". `base_price` and unreplayed CSV
prices are never shown.

Marker colours: grey = no price yet, blue = has a price; a dark ring marks a
price received in the last `--highlight-seconds` (default 5).

B's and C's files are unchanged on this branch. Sessions, occupancy and the
charging curve are on `main`.

## Database access

DuckDB allows only one process to write a file. With `--subscribe`, the
dashboard runs B's `PriceSubscriber` in a background thread and shares one
lock between its writes (C's `store_price_event`) and the dashboard's
read-only queries. Without `--subscribe` it only reads; do not run it against
a database that B's standalone subscriber is writing at the same time.

## Live demo (run from repository root)

```bash
python assignment2/dashboard/prepare_demo_db.py --force      # copy without price events
mosquitto -v                                                  # terminal 1
python assignment2/dashboard/app.py \
    --database assignment2/data/demo/demo_a2.duckdb --subscribe   # terminal 2
python assignment2/mqtt/publisher.py \
    --events assignment2/data/events/price_events.jsonl --interval 1   # terminal 3
```

Open http://127.0.0.1:8050/ and wait for "MQTT connected" before publishing.
`--interval 1` spreads the 60 price events over about a minute so each
change is easy to follow. Re-run `prepare_demo_db.py --force` before each
rehearsal; the submitted `data/final/project_a2.duckdb` already holds the
replay and is not modified.

## Checks

`python -m unittest discover -s assignment2/tests -v` — `test_dashboard.py`
feeds B's 60 events through the subscriber callback into a cleared copy of the
A2 database and checks: all 20 stations start with "No price yet"; after the
15:00 events each label equals C's `get_latest_price`; after all events every
station shows its 17:00 price, with 20 duplicates recognised and no storage
errors; and reads during writes cause no errors.

A live run on 21 October 2026 (local amqtt broker, B's publisher at
`--interval 0.3`) gave `published=60 failed=0`, 60 stored, 20/20 stations
priced, an open popup updating in place from $0.73 to $0.75, and no browser
console errors.
