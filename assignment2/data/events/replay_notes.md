# Price Update Replay Notes

Last updated: 21 October 2026

This document describes the data, processing rules, message format, and execution steps used for the first-stage `price_update` MQTT replay in Assignment 2. The current replay is intended for the Thursday group check-in and contains price events only. The `session_start`, `session_end`, and `meter` events will be added in later development stages.

## 1. Data source

The expected price source file in the group repository is:

```text
data/generated/sample_20/prices_hourly.csv
```

The local test used the same `prices_hourly.csv` dataset. It contains:

- 20 EV charging stations;
- 24 hourly prices for each station;
- 480 hourly price records in total; and
- timestamps from `2026-09-15T00:00:00` to `2026-09-15T23:00:00`.

The input fields are:

| Field | Meaning |
|---|---|
| `ts` | Event time associated with the price record |
| `station_id` | Station identifier corresponding to `record_id` in Assignment 1 |
| `price_per_kwh` | Simulated retail charging price in AUD/kWh |
| `wholesale_per_kwh` | AEMO wholesale electricity price in AUD/kWh |

The timestamps do not contain timezone information. The current programs preserve the original timestamps and do not convert them according to the timezone of the computer running the replay.

## 2. Event preparation

Price events are prepared by:

```text
mqtt/prepare_price_events.py
```

The script performs the following steps:

1. Verifies that the input CSV contains the four required columns.
2. Validates the station identifiers, timestamps, and numeric price values.
3. Sorts records by `station_id` and `ts`.
4. Retains the first observed price for each station.
5. Creates another event only when `price_per_kwh` differs from the previous price for that station.
6. Selects the configured replay window.
7. Generates a stable and unique `event_id` for each event.
8. Sorts all selected events by `event_time` and `station_id`.
9. Writes the prepared events to:

```text
data/events/price_events.jsonl
```

The 480 hourly price records for the full day produce 200 initial-price or price-change events after consecutive unchanged prices are removed.

## 3. Current replay window

The demonstration uses the following half-open time interval:

```text
2026-09-15T15:00:00 <= event_time < 2026-09-15T19:00:00
```

| Event time | Number of events |
|---|---:|
| `2026-09-15T15:00:00` | 20 |
| `2026-09-15T16:00:00` | 20 |
| `2026-09-15T17:00:00` | 20 |
| `2026-09-15T18:00:00` | 0, because no prices changed |
| **Total** | **60** |

All 20 stations have an effective price event at the beginning of this window, allowing the dashboard to establish a current price for each station. Each station then receives another price update at 16:00 and 17:00.

If a different dataset or replay window is used, the effective price at the start of the window must be checked again. If a station has no event at the start of the selected window, the necessary initial state must be supplied rather than displaying a future price.

## 4. JSON message format

Each JSONL line represents one independent `price_update` event:

```json
{
  "event_id": "price_ddfb524fabc22eaaf3e9ab0a",
  "event_type": "price_update",
  "event_time": "2026-09-15T15:00:00",
  "station_id": "ev_11a42ddd532302c6411c2070fdeebf4b3db5c058d31323e2042bfa60b576f721",
  "price_per_kwh": 0.64,
  "wholesale_per_kwh": 0.0056
}
```

| Field | Description |
|---|---|
| `event_id` | Stable identifier derived from the event type, station, and event time; used for duplicate detection and idempotent storage |
| `event_type` | Currently fixed as `price_update` |
| `event_time` | The original CSV `ts`; this is the data event time, not the wall-clock publication time |
| `station_id` | Original station identifier; it is not renumbered |
| `price_per_kwh` | Current simulated retail price |
| `wholesale_per_kwh` | Wholesale price for the corresponding hour |

Members B and C must confirm the field names and data types before database integration. Member C should use `event_id` to make database writes idempotent rather than relying only on the subscriber's in-memory duplicate detection.

## 5. MQTT configuration

| Setting | Value |
|---|---|
| Broker | Mosquitto 2.1.2 |
| Host | `localhost` |
| Port | `1883` |
| Username | None |
| Password | None |
| QoS | `1` |
| Retain | `false` |
| Delay between messages | `0.1` seconds |

The publication topic for each station is:

```text
evchargers/{station_id}/price
```

The subscriber uses the following wildcard topic:

```text
evchargers/+/price
```

Mosquitto currently runs in its default local-only mode, so only clients on the same computer can connect. An unauthenticated broker must not be exposed to the public internet or an untrusted network.

## 6. Environment setup

Create and activate a virtual environment from the project root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r assignment2/requirements.txt
```

The current Python MQTT dependency is:

```text
paho-mqtt==2.1.0
```

On macOS, install Mosquitto using Homebrew:

```bash
brew install mosquitto
```

Do not include the `.venv/` directory in the final submission package or Git repository.

## 7. Regenerating the event file

Run the following command from the project root after the sample data has been placed in the expected group-repository path:

```bash
python assignment2/mqtt/prepare_price_events.py \
  --input assignment2/data/generated/sample_20/prices_hourly.csv \
  --output assignment2/data/events/price_events.jsonl \
  --start 2026-09-15T15:00:00 \
  --end 2026-09-15T19:00:00
```

Expected summary:

```text
Input rows: 480
Input stations: 20
All retained initial/change events: 200
Output events: 60 across 20 stations
```

## 8. Starting and running the replay

Use three terminals and start the components in the following order.

### Terminal 1: Start the broker

```bash
mosquitto -v
```

Keep this terminal running so that connection, subscription, and publication activity remains visible.

### Terminal 2: Start the subscriber

```bash
cd /path/to/project
source .venv/bin/activate

python assignment2/mqtt/subscriber.py \
  --max-messages 60 \
  --output assignment2/data/events/received_price_events.jsonl
```

The subscriber validates the JSON structure, event type, field values, and consistency between the station identifier in the topic and the identifier in the payload. The `--max-messages 60` option stops the subscriber after it accepts 60 valid events. Without this option, press `Ctrl+C` to stop it.

### Terminal 3: Start the publisher

```bash
cd /path/to/project
source .venv/bin/activate

python assignment2/mqtt/publisher.py \
  --events assignment2/data/events/price_events.jsonl \
  --interval 0.1
```

Before publishing, the publisher checks the required fields, duplicate `event_id` values, and chronological event order. It publishes each message using QoS 1 and waits for an acknowledgement from the broker.

## 9. Completed tests

The local end-to-end test produced:

```text
publisher: published=60 failed=0
subscriber: accepted=60 duplicates=0 rejected=0
sent=60 received=60 exact_match=True
```

The test confirmed that:

- all 60 prepared events were published successfully;
- all 60 events were received successfully;
- no duplicate or invalid messages were detected;
- the received JSONL file matched the published JSONL file event by event; and
- each station received three different prices in the correct 15:00, 16:00, and 17:00 order.

## 10. Repeated replays and output files

`subscriber.py` appends accepted events to the file supplied through `--output` to avoid accidentally overwriting an earlier log. Before repeating the demonstration, use a new output filename or manually clear the old file after confirming that it is no longer required.

During one execution, the subscriber ignores repeated messages using `event_id`. This in-memory set is reset when the program restarts. The final database must therefore enforce a unique `event_id` or perform an equivalent idempotency check.

## 11. Current scope and next steps

The current stage has completed:

- conversion from hourly prices to `price_update` events;
- filtering of consecutive unchanged prices;
- JSONL event storage;
- MQTT publication using QoS 1;
- MQTT subscription, validation, and received-event logging; and
- an end-to-end test of all 60 replay events.

