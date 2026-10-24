# Dashboard (role D)



A Dash + dash-leaflet + Plotly page that shows the simulated stations on a
map and updates from the A2 DuckDB once per second (`--refresh-seconds`).
Everything on the page comes from events the subscriber has stored; the
generator CSVs and `base_price` are never read, so no future price or session
state can leak onto the page.

## What the page shows

Mapped to the assignment brief ("Data Subscription and Visualisation"):

| Brief | Implementation |
|---|---|
| Subscribe to the Task 2 MQTT messages | `--subscribe` runs B's subscriber in the dashboard process; each received message is stored and the page reads the database every second. |
| For each received message, dynamically add a marker at the charger's A1 location | The map starts empty. A station's marker is appended (Dash `Patch`) when its first message has been stored, at `charger_records.Latitude/Longitude`. Station IDs missing from A1 are counted in the status line, not guessed. |
| Each marker displays at least one dynamically updated attribute | A label next to each marker shows the latest price (`$0.63`), or plugs in use (`2/4`) in occupancy mode. Marker colour follows the same attribute (price: grey = no price yet, blue = has a price; occupancy: light → dark blue for free, partly in use, all plugs in use). A dark ring marks stations whose last price or session event arrived in the last `--highlight-seconds` (default 5). |
| Click a marker: pop-up with name, address, plug types, operator network, latest price | Name (marked "Missing in A1" with the generator's synthetic label, when an A1 name is unavailable), address, operator, plug types (connector types from A1's Open Charge Map augmentation, `attribute_connectors`; unmatched stations show "Missing" with A1's DC/AC type), plugs and power, latest price and its event time, plugs in use, completed sessions. |
| Full marks: live-updated curve of an active session's progress from meter events | When the station has an active session, the pop-up shows its cumulative energy (kWh) against event time and the latest kWh, kW and SoC; it grows with every stored meter reading and disappears when the session ends ("No active charging session."). With several active sessions the pop-up shows the most recently started one and says how many more there are. |
| Optional filters (not assessed) | Not implemented. |

The side panel repeats the details for the selected station, with metric
tiles, a session picker (active sessions first, then recent completed ones),
energy/power/state-of-charge curves, a step chart of received prices, and a
list of the latest price and session events. Empty states read "No price
received yet", "No sessions received for this station yet", "Waiting for the
first meter reading" and "No price history received yet".

Occupancy uses the generator's connector count (`stations.csv`
`connectors`, taken from A1 `Number_of_plugs`), falling back to A1
`Number_of_plugs`. "Active" means a `session_start` has been received and no
`session_end` yet, so a car that has finished charging but is still plugged in
counts as occupying its plug.

## Database access

The dashboard and its subscriber share one persistent DuckDB connection and one lock. Queries and writes are serialized, avoiding repeated database reopening for each event. The refresh timer pauses while a refresh request runs, preventing overlapping refresh requests from starving message processing. Shutdown stops the subscriber and checkpoints the database.

Without `--subscribe` the dashboard only reads. Do not run it against a
database that the standalone subscriber is writing at the same time; a read
can make a write fail with `STORE_ERROR`.

## Live demo (run from repository root)

1. Create a demo copy with no events. The submitted
   `data/final/project_a2.duckdb` already holds the full replay, so a map
   opened on it shows the final state immediately. The source is not modified.

   ```bash
   python assignment2/dashboard/prepare_demo_db.py --force
   ```

   This writes `assignment2/data/demo/demo_a2.duckdb` (ignored by Git).
   Re-run it before each rehearsal.

2. Start the broker: `mosquitto -v`

3. Start the dashboard with the in-process subscriber, open
   http://127.0.0.1:8050/ and wait for "MQTT connected" in the status line:

   ```bash
   python assignment2/dashboard/app.py \
     --database assignment2/data/demo/demo_a2.duckdb --subscribe
   ```

   Add `--received-log <file>.jsonl` to keep the received-event log.

4. Publish the four-type replay (1,482 events, at least 2.5 minutes at 0.1 s):

   ```bash
   python assignment2/mqtt/publisher.py \
     --events assignment2/data/events/replay_events.jsonl --interval 0.1
   ```

   For a price-only demo, publish `price_events.jsonl` instead, ideally with
   `--interval 1` so each change is easy to follow.

To view the submitted database without MQTT: `python assignment2/dashboard/app.py`.

## Checks

```bash
python -m unittest discover -s assignment2/tests -v
```

`tests/test_dashboard.py` feeds the real replay events through the subscriber
callback into a cleared copy of the A2 database (no broker or browser) and
checks: empty states before any event; after a partial replay to 01:02, each
station's active and completed sessions equal the counts derived from the
events themselves, every station shows its 01:00 price, the selected
station's sessions list active ones first and its readings stop before the
cutoff; replaying the same events again changes nothing; and dashboard reads
during writes cause no read or storage errors.

A local MQTT replay published and stored all 1,482 events with an exact payload match. The dashboard displayed 433 priced stations, 19 active sessions, 6 completed sessions and 585 meter readings, with no rejected messages or storage errors. The replay counts are recorded in `data/events/replay_validation.json`.

## Implementation notes

- dash-leaflet re-applies colours only when a marker's `pathOptions` changes,
  so style updates go through `pathOptions`; markers are created once and
  updated in place, and only stations whose state changed are sent.
- A popup bound to a marker loses its content in dash-leaflet once it has
  been closed and reopened, so each click mounts one standalone popup at the
  station and only its inner body is refreshed afterwards.
- The map's `trackViewport` is off: with `bounds` but no `center`, every pan
  raised an error in dash-leaflet that also broke popups.
- `DivMarker` does not update its icon after creation, so the value label is
  a permanent Leaflet tooltip whose inner text is updated in place.
- The pop-up reserves space for the curve when it opens on a station with an
  active session, so auto-pan brings the whole pop-up into view.
- `prepare_demo_db.py` drops and recreates the event tables instead of
  deleting rows: deleting from the earlier non-unique `meter_readings` index
  failed in DuckDB, so that index was removed from the schema.
