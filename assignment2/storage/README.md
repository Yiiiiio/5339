# Price storage: B / C handoff

The message input and Python query output use `event_time`. The source CSV
still uses `ts`; B's preparation script converts it to `event_time`.
The existing database column remains `event_ts`; C maps it internally, so
the A2 database does not need to be rebuilt for this interface change.

Required message fields:
`event_id`, `event_type`, `station_id`, `event_time`,
`price_per_kwh`, `wholesale_per_kwh`.

`event_type` is `price_update`. Preserve the original A1 `record_id` as
`station_id`. Preserve timezone-naive event times until the team agrees on
their timezone. Prices are AUD/kWh; wholesale prices may be negative.

## Windows commands (run from repository root)

Install A2 dependencies:

```bat
.venv\Scripts\python.exe -m pip install -r assignment2/requirements.txt
```

After starting a broker on localhost, start the subscriber first:

```bat
.venv\Scripts\python.exe assignment2/mqtt/subscriber.py --database assignment2/data/final/project_a2.duckdb --max-messages 60
```

Then run the publisher in another terminal:

```bat
.venv\Scripts\python.exe assignment2/mqtt/publisher.py --interval 0.1
```

The database path must already contain an A1 database copy. Initialization
adds the price table and view. Omitting `--database` preserves B's logging-only
mode. Only one process should own database writes; arrange dashboard access
with C before running a separate process against an open writable database.

## Results and errors

- `STORED`: the event was committed to DuckDB.
- `DUPLICATE`: the same event/content is already stored.
- `STORE_ERROR`: the write failed; it is not marked as processed.
  Inspect the error and replay after correcting the cause. Automatic retry and
  MQTT manual acknowledgment are not implemented.
- In database mode, `--max-messages` counts inserted plus database-duplicate
  messages so a complete replay of the same 60 events can finish.
- Same ID with different stored price/time/station, or conflicting prices for
  the same station and time, produces an error.
- New event times with equal prices are retained if received; B handles
  filtering consecutive unchanged prices.
- `get_latest_price(path, station_id)` returns a dictionary or `None`.
  `get_price_history(path, station_id)` returns dictionaries in event-time order.
  Both expose `event_time` and numeric price values.

## Offline checks

```bat
.venv\Scripts\python.exe -m unittest discover -s assignment2/tests -v
```

The integration check uses B's 60 JSONL events, the subscriber callback, and a
temporary copy of the real A1 database. It checks 60 initial writes, 60
duplicates after restarting the subscriber, 20 latest prices at 17:00,
conflict detection and manual retry after a failed write.
It does not establish a live MQTT connection or modify the submitted A2 DB.

# Session and meter storage (four event types)

`storage/session_store.py` adds `session_start`, `session_end` and `meter`
storage to the same A2 database; the DDL is appended to `sql/a2_schema.sql`
and is applied by the same `initialise_database` call. The subscriber now
calls `store_event(path, event)`, which sends `price_update` to
`store_price_event` and the other types to `store_session_event` /
`store_meter_event`.

Tables and views:

- `session_events`: received `session_start` / `session_end` events, one per
  session and type (`UNIQUE (session_id, event_type)`). Final energy, cost and
  charge-end time are columns that only `session_end` fills.
- `meter_readings`: received readings, `UNIQUE (session_id, event_ts)`.
  `energy_kwh` is cumulative per session and is never summed across rows.
- `session_state` (view): one row per session; `active` while no end has been
  received, then `completed`. An end that arrives before its start still
  yields one completed session.
- `station_session_metrics` (view): active sessions, occupied connectors,
  completed sessions, completed kWh and revenue per station.

Metrics are derived from stored events instead of counters, so a duplicate
message cannot be counted twice and a replay of the same file changes nothing.

Duplicate and conflict rules follow the price stage: same `event_id` and
content, or same natural key and content, returns `duplicate`; same ID or key
with different content raises `ValueError`; so does a session whose start,
end or readings name a different station or connector, an unknown station,
or `soc_pct` above 100.

Queries for the dashboard: `get_station_session_metrics(path, station_id)`,
`get_station_sessions(path, station_id)` (active first, then recent completed,
with reading counts) and `get_session_readings(path, session_id)` in event-time
order.

Each write opens one short DuckDB connection; on the development machine a
stored event took about 18 ms, well inside the 0.1 s publishing interval.
Checks: `tests/test_session_store.py` and `tests/test_dashboard.py`.
