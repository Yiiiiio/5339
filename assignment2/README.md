# COMP5339 Assignment 2

EV Charging Data Streaming and Analysis

This project uses Assignment 1 charger data to generate synthetic hourly prices, charging sessions and meter readings. Events are published through MQTT, stored in DuckDB and displayed on a live map-based dashboard. The submitted dataset contains 433 DC station records, 10,392 hourly prices, 4,027 sessions and 123,025 meter readings.

## 1. Project structure

```text
README.md
START_DASHBOARD.command
Data_Preparation_and_Generation.ipynb
requirements.txt
generator/
mqtt/
storage/
dashboard/
scripts/
sql/a2_schema.sql
data/
  input/
  generated/dc_data/
  events/
  final/project_a2.duckdb
a1_dependencies/
  raw/
  processed/
  external/
  final/project.duckdb
  schema.sql
```

The notebook prepares inputs, runs the generator and validates its outputs. `generator/` contains the simulation model; `mqtt/` prepares, publishes and receives events; `storage/` and `sql/` define storage and queries; `dashboard/` displays the map, prices and charging curves; and `scripts/` starts the application.

The `a1_dependencies/` folder contains the A1 data and schema needed for this submission. All required files are included within this project folder; no parent repository or external A1 folder is required.

## 2. Setup instructions

Use Python 3.12 for the application and notebook environment. Open a terminal in the project folder and create a virtual environment.

On macOS or Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

On Windows Command Prompt:

```bat
py -3.12 -m venv .venv
.venv\Scripts\activate
```

Install all application and notebook dependencies from the single requirements file:

```bash
python -m pip install -r requirements.txt
```

The launcher includes a local MQTT broker; no separate broker installation is required. Dependency installation and online map tiles require network access. A2 data generation uses the included AEMO cache.

## 3. Execution instructions

### 3.1 Run the supplied event stream

After installing dependencies, run:

```bash
python scripts/run_dashboard.py
```

The launcher creates a fresh runtime database from the submitted A2 database, clears its event tables, starts the broker on `127.0.0.1:1883` and opens the dashboard at `http://127.0.0.1:8050`. Once the subscription is ready, press Enter in the terminal to publish the events. Keep the terminal open; press Ctrl+C to stop the services. Ports 1883 and 8050 must be available.

On macOS, `START_DASHBOARD.command` starts the same application after environment setup. VS Code also provides the **Start charging dashboard** task.

Markers appear as events are received. Click a marker to see station details, latest prices, occupancy and an active charging curve. The side panel shows session selection, charging curves, price history and completed-session metrics.

The replay contains 1,482 events: 866 price updates, 25 session starts, 585 meter readings and 6 session ends. Messages use JSON and MQTT QoS 1, with at least 0.1 seconds between publications. Publication therefore takes at least about 2.5 minutes. At the end, the expected state is 433 stations with prices, 19 active sessions and 6 completed sessions.

Results and logs are saved in `data/runtime/`. Each launch resets this runtime database. The submitted `data/final/project_a2.duckdb` is preserved.

### 3.2 View the submitted database without replay

```bash
python dashboard/app.py
```

This displays the stored results in `data/final/project_a2.duckdb` without starting MQTT publication. Use this command when inspecting the final stored state.

### 3.3 Regenerate the A2 data and event files

First, open the notebook and run all cells in order:

```bash
python -m jupyterlab Data_Preparation_and_Generation.ipynb
```

The notebook reads `a1_dependencies/processed/chargers_clean.csv`, selects all 433 DC records and preserves `record_id` as `station_id`. It prepares the generator input, generates one day of data for 15 September 2026 with seed 5339 and urban/regional rates of 3.1/2.1 sessions per connector per day, then validates identifiers, counts, timing and meter readings. The included AEMO cache must remain in `data/generated/dc_data/aemo_cache/`. Running all cells overwrites the input and generated output files.

Next, regenerate the event files, then start the application:

```bash
python mqtt/prepare_price_events.py
python mqtt/prepare_replay_events.py
python scripts/run_dashboard.py
```

The event files are prepared for `[00:55, 01:05)` on 15 September 2026. Effective initial prices and original starts and meter history for sessions already active at the window start are included. Initialization events can therefore occur before 00:55. The complete one-day generated datasets remain included; only the event replay uses this shorter window.

## 4. Assumptions and limitations

- A station identifier represents an A1 source record, not an independently verified physical site. Duplicate-review flags and known coordinate concerns are retained.
- Existing A1 power values are used. Six missing values are recovered from original ratings, using the lower explicitly stated kW rating where needed. Missing names use operator and address, shortened to 90 characters.
- Urban/regional categories are simulation conventions. Plug counts represent independent simulated connectors, not verified simultaneous charging capacity.
- Prices and sessions are synthetic. The course-provided model combines real AEMO NSW1 wholesale prices with a hypothetical retail tariff, stochastic arrivals and approximate vehicle charging behaviour. Hourly arrival profiles, fleet mix and derating are estimates, not observations of these stations.
- Timestamps retain the generator's timezone-naive format. Prices are AUD/kWh; wholesale prices may be negative. Meter `energy_kwh` is cumulative within a session and must not be summed across readings.
- An active session has a received start and no received end. It remains occupied until the end event, including time after charging has stopped but before unplugging.

### Data and model sources

- [Transport for NSW EV charging locations](https://opendata.transport.nsw.gov.au/data/dataset/ev-charging-locations)
- [ABS ASGS Edition 4 boundary files](https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs/edition-4-july-2026-june-2031/access-and-downloads/digital-boundary-files)
- [AEMO aggregated price and demand data](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/aggregated-data)
- [ARENA charging-network insights](https://arena.gov.au/knowledge-bank/insights-from-ultra-fast-charging-network-data) (Energeia, August 2023; CC BY 3.0 AU)
- [NRMA EV information](https://mynrma.com.au/ev), [location-based pricing reference](https://zecar.com/reviews/nrma-ev-charging-network-introduces-tap-n-pay-and-location-based-pricing-at-some-locations), and [Chargefox tariff trial reference](https://4x4australia.com.au/news/chargefox-trials-wild-fluctuating-ev-charging-prices)

The simulation uses the course-provided `generate_dataset.py` model. The tariff references describe its calibration context; generated prices are not current operator quotations. A1 external attribute responses and matching records are included in `a1_dependencies/external/`.

## 5. Outputs and included data files

### A2 inputs and generated datasets

| File | Contents |
|---|---|
| `data/input/stations.csv` | Generator input: 433 stations |
| `data/input/stations_preparation.csv` | 433 per-record preparation and review decisions |
| `data/input/stations_metadata.json` | Source identifiers, classification rules and assumptions |
| `data/generated/dc_data/stations.csv` | 433 station records with simulated base prices |
| `data/generated/dc_data/prices_hourly.csv` | 10,392 hourly retail and wholesale prices |
| `data/generated/dc_data/sessions.csv` | 4,027 sessions with times, energy and cost |
| `data/generated/dc_data/session_metering.csv` | 123,025 voltage, current, power, energy and state-of-charge readings |
| `data/generated/dc_data/run_summary.json` | Generation parameters, validation results and SHA-256 hashes |
| `data/generated/dc_data/aemo_cache/` | AEMO NSW1 wholesale-price source for September 2026 |

### Events, databases and schema

| File | Contents |
|---|---|
| `data/events/replay_events.jsonl` | 1,482 time-ordered MQTT events of all four types |
| `data/events/price_events.jsonl` | The 866-event price subset |
| `data/events/replay_validation.json` | Recorded publication, reception and storage validation |
| `data/events/replay_notes.md` | Event fields, topics and replay-window rules |
| `data/final/project_a2.duckdb` | Submitted A2 database: A1 tables plus 866 price events, 31 session events and 585 meter readings, with current-state and historical views |
| `sql/a2_schema.sql` | DDL for A2 event tables, constraints and views; apply to a database containing the A1 tables |
| `data/runtime/replay.duckdb` | Created at runtime for fresh replay; reset on each launcher run and excluded from Git |
| `data/runtime/` | Runtime received-event log and service logs |

The A2 database contains the selected replay, not every generated session and meter reading. The full generated datasets are supplied separately as CSV files.

### Assignment 1 dependencies

| File | Contents |
|---|---|
| `a1_dependencies/processed/chargers_clean.csv` | 1,958 cleaned source records; A2 selects all 433 DC records |
| `a1_dependencies/processed/charger_sa4.csv` | SA4 matching results for all source records |
| `a1_dependencies/processed/charger_sa4_review.csv` | Unresolved spatial-match candidates |
| `a1_dependencies/processed/charger_attributes.csv` | 216 accepted external augmentation records |
| `a1_dependencies/final/project.duckdb` | A1 database with charger records, regions, spatial results and external attributes |
| `a1_dependencies/raw/` | Original December 2025 charger CSV and 2026 SA4 boundary ZIP |
| `a1_dependencies/external/` | Cached Open Charge Map responses and candidate-review CSV files |
| `a1_dependencies/schema.sql` | DDL for the A1 database |

The A1 database contains 1,958 source records, 1,957 confirmed SA4 assignments, one unmatched record and 216 augmentation records. A2 retains these tables and links received events through station identifiers.
