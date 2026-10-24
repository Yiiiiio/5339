# Assignment 2 EV Charging Data Streaming and Analysis

## Data preparation and generation

Run `Data_Preparation_and_Generation.ipynb` from within the repository using Python 3.9 or later. The notebook uses the Python standard library and the supplied, unchanged `generator/generate_dataset.py`. It reads `../data/processed/chargers_clean.csv`, selects all 433 DC records in source order, and preserves A1 `record_id` as `station_id`.

The simulation covers 15 September 2026 for one day, with seed 5339 and urban/regional session rates of 3.1/2.1 per connector per day. The included AEMO NSW1 cache supports offline reproduction. Run all cells to prepare inputs, generate data, validate relationships and times, and save provenance records. Rerunning overwrites these generated outputs.

| File | Purpose | Rows |
|---|---|---:|
| `data/input/stations.csv` | Seven-field generator input | 433 |
| `data/input/stations_preparation.csv` | Per-record preparation and review decisions | 433 |
| `data/input/stations_metadata.json` | Source, classification rules and assumptions | — |
| `data/generated/dc_data/stations.csv` | Station attributes and base prices | 433 |
| `data/generated/dc_data/prices_hourly.csv` | Hourly retail and wholesale prices in AUD/kWh | 10,392 |
| `data/generated/dc_data/sessions.csv` | Charging-session times, energy and cost | 4,027 |
| `data/generated/dc_data/session_metering.csv` | Meter telemetry and cumulative session energy | 123,025 |
| `data/generated/dc_data/run_summary.json` | Parameters, checks, counts and SHA-256 hashes | — |
| `data/generated/dc_data/aemo_cache/` | AEMO monthly wholesale-price source | — |

Existing A1 power values are retained. Six missing power values are recovered from original ratings, taking the lower explicitly stated kW value where needed. Missing names use operator and address, with labels limited to 90 characters. Urban/regional categories are documented simulation conventions. Source records are not independently verified physical sites; duplicate-review flags and known coordinate concerns are retained in the preparation records. Plug counts model independent connectors and do not establish simultaneous physical capacity.

Validation checks hourly coverage, identifier references, connector ranges, session timing, non-overlapping sessions and nondecreasing cumulative meter energy. All 433 IDs also exist in the A1 database. Meter `energy_kwh` is cumulative and must not be summed across readings. Timestamps are preserved without timezone conversion.

## Project structure

- `generator/`: supplied simulation script and documentation.
- `mqtt/`: event preparation, JSON schemas, publishing and subscription.
- `storage/` and `sql/`: DuckDB event storage, queries and schema.
- `dashboard/`: map, prices, session metrics and charging curves.
- `tests/`: event, storage and dashboard checks.

## Setup and execution

Use Python 3.10 or later. From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r assignment2/requirements.txt
python assignment2/scripts/run_demo.py
```

On Windows, activate with `.venv\Scripts\activate` instead. On macOS, `START_DEMO.command` launches the same program after environment setup. VS Code provides the **Start charging dashboard** task.

The launcher creates a clean database copy, starts the local MQTT broker and dashboard, waits for the subscription acknowledgement, and opens http://127.0.0.1:8050. Press Enter in the terminal to replay the events. Keep the terminal open; Ctrl+C stops the services. The stored results remain in `data/demo/demo_a2.duckdb`. The source database in `data/final` is not reset by the launcher.

To view the included database without starting MQTT:

```bash
python assignment2/dashboard/app.py
```

## Event preparation

```bash
python assignment2/mqtt/prepare_price_events.py
python assignment2/mqtt/prepare_replay_events.py
```

Both commands read `data/generated/dc_data`. The replay interval is 00:55–01:05 on 15 September 2026. Initial prices and earlier start/meter events for ongoing sessions are included with their original timestamps. This provides visible price changes and session activity while keeping replay duration manageable.

| Event type | Count |
|---|---:|
| `price_update` | 866 |
| `session_start` | 25 |
| `meter` | 585 |
| `session_end` | 6 |
| Total | 1,482 |

Messages use JSON payloads and MQTT QoS 1 with at least 0.1 seconds between publications. Preparing files does not send messages. `data/events/replay_events.jsonl` contains all four event types; `price_events.jsonl` is its price-only subset. Use a clean replay database when changing generation settings or the replay window.

## Storage and visualization

The subscriber validates each event and writes it to DuckDB, preserving the A1 `charger_records` identifiers. Prices, session events and meter readings are stored with duplicate/conflict checks. The dashboard queries received data for current prices, occupied connectors, completed sessions, historical prices and charging curves. Map markers appear after their first event is received.

See [dashboard instructions](dashboard/README.md), [storage instructions](storage/README.md) and [event documentation](data/events/replay_notes.md).

## Verification

```bash
python -m unittest discover -s assignment2/tests -v
```

The tests check event preparation against committed replay files, message schemas, duplicate/conflict handling, session state, price history, concurrent access and dashboard construction. The data generation notebook separately validates the generated datasets.
