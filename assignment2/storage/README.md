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
