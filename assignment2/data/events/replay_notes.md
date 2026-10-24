# MQTT Event Replay

## Data and window

Source files are `assignment2/data/generated/dc_data/prices_hourly.csv`, `sessions.csv` and `session_metering.csv`. The simulation contains 433 station records, 10,392 hourly prices, 4,027 sessions and 123,025 meter readings.

The replay window is `[2026-09-15T00:55:00, 2026-09-15T01:05:00)`. Each station contributes its effective initial price and subsequent changes. Sessions overlapping the window contribute their original starts and meter history up to the window end. End events are included only when the session ends before the window end. Original timestamps are preserved, so initialization events can precede 00:55.

## Files and counts

`replay_events.jsonl` contains 1,482 events: 866 prices, 25 session starts, 585 meter readings and 6 session ends. `price_events.jsonl` contains the 866 price events. Each JSONL line is one event; the publisher sends one JSON payload per MQTT message.

## Message design

Common fields are `event_id`, `event_type`, `event_time` and `station_id`. Session messages also include `session_id` and `connector_id`. Starts include only information known at the start, such as vehicle model and price. Ends contain final energy and cost. Meter messages contain voltage, current, power, cumulative energy and state of charge.

Topics use `evchargers/{station_id}/{suffix}` with suffixes defined in `mqtt/event_schema.py`. The subscriber listens to `evchargers/+/+`. Events are ordered by time and event-type priority so session starts precede readings and ends at equal timestamps.

## Reproduction

From the repository root:

```bash
python assignment2/mqtt/prepare_price_events.py
python assignment2/mqtt/prepare_replay_events.py
python assignment2/scripts/run_demo.py
```

The local broker listens on 127.0.0.1:1883. Publishing uses QoS 1, no retained messages, and a minimum 0.1-second interval. A broker acknowledgement alone is not proof of database insertion; verify accepted-event and stored-event counts. The launcher waits for a successful subscription acknowledgement before publication.

The subscriber rejects invalid messages and reports storage errors. Duplicate events do not add duplicate database records. Event identifiers remain stable for the same event identity; changing simulation values requires a clean database to avoid conflicts with earlier values at the same station and time.
