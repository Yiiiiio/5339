# Event Storage

The subscriber stores validated MQTT events in DuckDB. Station identifiers reference `charger_records.record_id` from Assignment 1. The schema is defined in `../sql/a2_schema.sql` and applied by `initialise_database` to a database containing the A1 tables.

## Tables and views

| Object | Purpose |
|---|---|
| `price_events` | Price-update history and original payloads |
| `latest_station_price` | Most recent received price by event time for each station |
| `session_events` | Session-start and session-end history |
| `meter_readings` | Session telemetry, including cumulative energy |
| `session_state` | One derived active or completed state per session |
| `station_session_metrics` | Active sessions, occupied connectors, completed sessions, energy and revenue |

Messages and Python query results use `event_time`; database columns use `event_ts`. Timestamps retain the generator's timezone-naive representation. Prices are AUD/kWh; wholesale prices may be negative. Meter energy is cumulative and must not be summed across readings.

## Validation and duplicate handling

- Identical event IDs and content return `duplicate` without inserting another row.
- Conflicting content for the same ID or natural key raises an error.
- Natural keys are station/time for prices, session/type for session events, and session/time for readings.
- A session's start, end and readings must reference the same station and connector.
- Unknown stations and invalid numerical values are rejected.
- Session state is derived from stored history, so receiving an end before its start still produces one completed session.
- Price-change filtering occurs during event preparation; the storage layer preserves valid received price events.

The subscriber reports `STORED`, `DUPLICATE` or `STORE_ERROR`. Storage failures are not marked as processed. Correct the cause and replay failed events; automatic retry and manual MQTT acknowledgement are not implemented.

## Queries

`price_store.py` exposes `get_latest_price` and `get_price_history`. `session_store.py` exposes `get_station_session_metrics`, `get_station_sessions` and `get_session_readings`. Session lists return active sessions first, followed by recent completed sessions; the default list limit is 20. This limit does not affect ingestion, stored history or aggregate metrics.

## Execution

The application launcher coordinates the broker, subscriber, database and dashboard:

```bash
python assignment2/scripts/run_dashboard.py
```

For standalone ingestion, start a local broker and use an A1 database copy as the target. Start the subscriber before the publisher:

```bash
python assignment2/mqtt/subscriber.py --database path/to/receiving.duckdb --max-messages 1482
python assignment2/mqtt/publisher.py --events assignment2/data/events/replay_events.jsonl --interval 0.1
```

Run these commands in separate terminals. Omitting `--database` enables logging without storage. One process must own database writes; the integrated application shares its connection and lock with dashboard queries.

## Tests

```bash
python -m unittest discover -s assignment2/tests -v
```

Tests cover ingestion, duplicate handling, conflicts, late events, session lifecycle, metrics and concurrent dashboard access. Tests use temporary databases and leave the submitted database unchanged.
