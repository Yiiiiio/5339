# COMP5339 EV Charging Data Streaming and Analysis

Assignment 2 integrates synthetic charging data, MQTT event streaming, DuckDB storage and an interactive dashboard. The repository includes the Assignment 1 source data and database used by Assignment 2.

All paths and commands below are relative to the repository root. Keep the repository structure when extracting the submission. In particular, the A2 notebook and tests depend on `data/processed/chargers_clean.csv` and `data/final/project.duckdb`.

The data preparation notebook is [Data_Preparation_and_Generation.ipynb](assignment2/Data_Preparation_and_Generation.ipynb).

## Data preparation and generation

Run `assignment2/Data_Preparation_and_Generation.ipynb` from within the repository using Python 3.10 or later. The notebook uses the Python standard library and the course-provided simulation model in `assignment2/generator/generate_dataset.py`. It reads `data/processed/chargers_clean.csv`, selects all 433 DC records in source order, and preserves A1 `record_id` as `station_id`.

The simulation covers 15 September 2026 for one day, with seed 5339 and urban/regional session rates of 3.1/2.1 per connector per day. The included AEMO NSW1 cache supports offline reproduction. Run all cells to prepare inputs, generate data, validate relationships and times, and save provenance records. Rerunning overwrites these generated outputs.

| File | Purpose | Rows |
|---|---|---:|
| `assignment2/data/input/stations.csv` | Seven-field generator input | 433 |
| `assignment2/data/input/stations_preparation.csv` | Per-record preparation and review decisions | 433 |
| `assignment2/data/input/stations_metadata.json` | Source, classification rules and assumptions | — |
| `assignment2/data/generated/dc_data/stations.csv` | Station attributes and base prices | 433 |
| `assignment2/data/generated/dc_data/prices_hourly.csv` | Hourly retail and wholesale prices in AUD/kWh | 10,392 |
| `assignment2/data/generated/dc_data/sessions.csv` | Charging-session times, energy and cost | 4,027 |
| `assignment2/data/generated/dc_data/session_metering.csv` | Meter telemetry and cumulative session energy | 123,025 |
| `assignment2/data/generated/dc_data/run_summary.json` | Parameters, checks, counts and SHA-256 hashes | — |
| `assignment2/data/generated/dc_data/aemo_cache/` | AEMO monthly wholesale-price source | — |

Existing A1 power values are retained. Six missing power values are recovered from original ratings, taking the lower explicitly stated kW value where needed. Missing names use operator and address, with labels limited to 90 characters. Urban/regional categories are documented simulation conventions. Source records are not independently verified physical sites; duplicate-review flags and known coordinate concerns are retained in the preparation records. Plug counts model independent connectors and do not establish simultaneous physical capacity.

Validation checks hourly coverage, identifier references, connector ranges, session timing, non-overlapping sessions and nondecreasing cumulative meter energy. All 433 IDs also exist in the A1 database. Meter `energy_kwh` is cumulative and must not be summed across readings. Timestamps are preserved without timezone conversion.

## Project structure

- `assignment2/generator/`: course-provided simulation model.
- `assignment2/mqtt/`: event preparation, JSON schemas, publishing and subscription.
- `assignment2/storage/` and `assignment2/sql/`: DuckDB event storage, queries and schema.
- `assignment2/dashboard/`: map, prices, session metrics and charging curves.
- `assignment2/tests/`: event, storage and dashboard checks.

## Setup and execution

Use Python 3.10 or later. From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r assignment2/requirements.txt
python assignment2/scripts/run_dashboard.py
```

On Windows, activate with `.venv\Scripts\activate` instead. On macOS, `START_DASHBOARD.command` launches the same program after environment setup. VS Code provides the **Start charging dashboard** task.

The launcher creates a clean database copy, starts the local MQTT broker and dashboard, waits for the subscription acknowledgement, and opens http://127.0.0.1:8050. Press Enter in the terminal to replay the events. Keep the terminal open; Ctrl+C stops the services. The stored results remain in `assignment2/data/runtime/replay.duckdb`. The source database in `assignment2/data/final` is not reset by the launcher.

To view the included database without starting MQTT:

```bash
python assignment2/dashboard/app.py
```

To open and execute the data preparation notebook, install the notebook environment:

```bash
python -m pip install -r assignment2/requirements-notebook.txt
python -m jupyterlab assignment2/Data_Preparation_and_Generation.ipynb
```

## Event preparation

```bash
python assignment2/mqtt/prepare_price_events.py
python assignment2/mqtt/prepare_replay_events.py
```

Both commands read `assignment2/data/generated/dc_data`. The replay interval is 00:55–01:05 on 15 September 2026. Initial prices and earlier start/meter events for ongoing sessions are included with their original timestamps. This provides visible price changes and session activity while keeping replay duration manageable.

| Event type | Count |
|---|---:|
| `price_update` | 866 |
| `session_start` | 25 |
| `meter` | 585 |
| `session_end` | 6 |
| Total | 1,482 |

Messages use JSON payloads and MQTT QoS 1 with at least 0.1 seconds between publications. Preparing files does not send messages. `assignment2/data/events/replay_events.jsonl` contains all four event types; `price_events.jsonl` is its price-only subset. Use a clean replay database when changing generation settings or the replay window.

## Storage and visualization

The subscriber validates each event and writes it to DuckDB, preserving the A1 `charger_records` identifiers. Prices, session events and meter readings are stored with duplicate/conflict checks. The dashboard queries received data for current prices, occupied connectors, completed sessions, historical prices and charging curves. Map markers appear after their first event is received.

The A2 schema is defined in [a2_schema.sql](assignment2/sql/a2_schema.sql). See [event documentation](assignment2/data/events/replay_notes.md) for message formats and replay-window rules.

## Verification

```bash
python -m unittest discover -s assignment2/tests -v
```

The tests check event preparation against committed replay files, message schemas, duplicate/conflict handling, session state, price history, concurrent access and dashboard construction. The data generation notebook separately validates the generated datasets.

## Dashboard interactions

- A marker appears when the station's first event is stored. Its label displays the latest price or occupied connectors, according to the selected mode.
- Clicking a marker shows the station name, address, operator, available plug types, price and occupancy. Missing source attributes are identified rather than inferred.
- An active session's popup contains a live cumulative-energy curve and its latest energy, power and state of charge. With multiple active sessions, the popup displays the most recently started session.
- The side panel provides a session selector, charging curves, historical prices, completed-session metrics and recent events.
- An active session has a received start and no received end. A vehicle that has stopped charging but remains plugged in still occupies its connector.

Prices and session state are derived only from received events. Initial empty states remain visible until the corresponding data arrives. Meter energy is cumulative within a session and is not summed across readings.

### Database access

The dashboard and subscriber share a persistent DuckDB connection protected by a lock. The refresh timer pauses while a refresh is in progress. Shutdown stops the subscriber and checkpoints the database. Do not run a standalone writer against a database currently open in a separate dashboard process.

## Event storage tables and queries

| Object | Purpose |
|---|---|
| `price_events` | Price-update history and original payloads |
| `latest_station_price` | Most recent received price by event time for each station |
| `session_events` | Session-start and session-end history |
| `meter_readings` | Session telemetry, including cumulative energy |
| `session_state` | One derived active or completed state per session |
| `station_session_metrics` | Active sessions, occupied connectors, completed sessions, energy and revenue |

Messages and Python query results use `event_time`; database columns use `event_ts`. Timestamps retain the generator's timezone-naive representation. Prices are AUD/kWh; wholesale prices may be negative. Meter energy is cumulative and must not be summed across readings.

### Event validation

- Identical event IDs and content return `duplicate` without inserting another row.
- Conflicting content for the same ID or natural key raises an error.
- Natural keys are station/time for prices, session/type for session events, and session/time for readings.
- A session's start, end and readings must reference the same station and connector.
- Unknown stations and invalid numerical values are rejected.
- Session state is derived from stored history, so receiving an end before its start still produces one completed session.
- Price-change filtering occurs during event preparation; the storage layer preserves valid received price events.

The subscriber reports `STORED`, `DUPLICATE` or `STORE_ERROR`. Storage failures are not marked as processed. Correct the cause and replay failed events; automatic retry and manual MQTT acknowledgement are not implemented.

### Queries

`assignment2/storage/price_store.py` exposes `get_latest_price` and `get_price_history`. `assignment2/storage/session_store.py` exposes `get_station_session_metrics`, `get_station_sessions` and `get_session_readings`. Session lists return active sessions first, followed by recent completed sessions; the default list limit is 20. This limit does not affect ingestion, stored history or aggregate metrics.

## Simulation model and sources

The course-provided model uses the following pricing and charging assumptions. Tariff figures below are calibration references rather than current tariff quotations.

**Prices.** NRMA does **not** use time-of-use or dynamic pricing. It charges a flat $/kWh by charger
power (about $0.54–0.60 up to 150 kW, $0.59–0.65 at 175 kW and above, $0.41–0.45 AC; members get about
10% off). Since April 2025 it has also used *location-based* prices, up to about $0.79/kWh.
The generator therefore models a **hypothetical dynamic tariff**:
`price = station_base + pass_through × (hourly AEMO NSW1 RRP − period mean) + $0.10 (4–9pm)`,
clipped to $0.20–1.20/kWh. This range matches real Australian time-of-use schemes: the Chargefox SA
trial charged $0.12–1.20/kWh, and Tesla Superchargers range from about $0.43 to $0.69. Use `--static-prices` for NRMA-style flat prices.

**Sessions.** Starts follow a non-homogeneous Poisson process per connector:
- **Hourly shape:** taken from the ARENA charts. Starts are mostly mid-day, and urban sites get a weekday 6–10pm bump.
- **Volume:** the model defaults are 4 urban and 3 regional sessions per connector per day. This project uses 3.1 and 2.1 respectively.
- **Price response:** demand changes with the price ratio, with elasticity −0.5.
- **Busy connectors:** a driver who arrives at an occupied connector goes elsewhere.

**Charging physics.** Each session uses one vehicle from an approximate Australian fleet mix
(battery size, maximum DC kW, 400 V or 800 V architecture). It arrives at 20–55% state of charge and
targets about 74%. Power is limited by the charger's rating, the charger's current limit, the vehicle's
taper curve above 50% SoC, and a real-world derating factor. Pack voltage rises with SoC, and current = P/V.

Model limitations: the hourly start profile, the fleet mix and the derating are approximations read from charts
or estimated. They are not published figures.

### Sources
- ARENA / Energeia, *Insights from Ultra-Fast Charging Network Data (Update)*, Aug 2023 (CC BY 3.0 AU): https://arena.gov.au/knowledge-bank/insights-from-ultra-fast-charging-network-data
- NRMA EV charging prices: https://mynrma.com.au/ev ; location-based pricing: https://zecar.com/reviews/nrma-ev-charging-network-introduces-tap-n-pay-and-location-based-pricing-at-some-locations
- Chargefox time-of-use trial: https://4x4australia.com.au/news/chargefox-trials-wild-fluctuating-ev-charging-prices

## Assignment 1 data pipeline

### 1. Project Structure

- `notebooks/` contains four notebooks for data acquisition and cleaning, spatial integration, data augmentation, and database construction.
- `scripts/build_database.py` builds and validates the DuckDB database.
- `sql/schema.sql` defines the database tables, constraints, and views.
- `data/raw/` stores the original charger CSV and SA4 boundary ZIP.
- `data/processed/` stores the cleaned data, SA4 assignments, spatial review records, and augmentation results.
- `data/external/` stores the external API responses and matching review files. 
- `data/final/` contains the final DuckDB database.
- `requirements.txt` lists the required Python packages.
- `README.md` explains the project structure, environment setup, execution steps, assumptions, and output files.
  
### 2. Setup Instructions

Install Python 3.11 before following the steps below.
Open a terminal in the project root folder.

On macOS or Linux:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

On Windows (Command Prompt):

```bat
py -3.11 -m venv .venv
.venv\Scripts\activate
```

Then install the dependencies and start Jupyter:

```bash
python -m pip install -r requirements.txt
python -m ipykernel install --user --name comp5339 --display-name "Python (COMP5339)"
jupyter lab
```

Select the `Python (COMP5339)` kernel when opening the notebooks.

### 3. Execution Instructions

Start JupyterLab from the project root. Open the notebooks in `notebooks/` and select the `Python (COMP5339)` kernel. Run all cells in each notebook in the following order:

1. `01_acquire_clean.ipynb`: Downloads and cleans the charger data, downloads and extracts the SA4 boundaries, and saves `data/processed/chargers_clean.csv`.
2. `02_spatial.ipynb`: Assigns chargers to SA4 regions and saves `charger_sa4.csv` and `charger_sa4_review.csv` in `data/processed/`.
3. `03_augment.ipynb`: Uses Open Charge Map data to add charger attributes and saves `data/processed/charger_attributes.csv`. It uses local API caches when available; otherwise, an API key is required. New API data may change the results.
4. `04_database.ipynb`: Builds `data/final/project.duckdb` and runs validation and example queries.

Keep the original folder structure. The first notebook expects its working directory to be `notebooks/`. Rerunning the notebooks overwrites generated files.

#### Database Rebuild

To rebuild only the database, keep the four CSV files in `data/processed/` and the SA4 ZIP in `data/raw/`. Run `04_database.ipynb`, or run this command from the project root:

```bash
python scripts/build_database.py
```

An Open Charge Map API key is not required for this step.

#### Validation

Run the tests from the project root:

```bash
python -m unittest discover -s tests -v
```

With the supplied data, the database should contain 1,958 charger records and 216 augmentation records, with 1,957 confirmed SA4 assignments and one unmatched record.

### 4. Assumptions Made

- Each `record_id` represents one source record, not necessarily one physical charging site. Records sharing coordinates or addresses are retained and flagged for review.
- Charger coordinates are assumed to use EPSG:4326. They are transformed to match the SA4 boundaries in EPSG:7844 before spatial matching. Unmatched records remain unassigned.
- Missing values are not treated as zero. Postcodes extracted from addresses are marked as inferred, while conflicting postcode values remain unresolved.
- Only single power values with an explicit kW unit are converted to `power_kw`. Other formats are preserved separately. `Upcoming` is treated as a status, not a charger type.
- Operator aliases are merged only where supporting evidence is available. Ambiguous names are retained for review.
- Augmentation covers DC records. For coverage calculation, locations are grouped by coordinates rounded to six decimal places. This grouping does not guarantee that each location represents a separate physical site.
- External matches use location and other available attributes. Accepted matches may still contain errors, especially where source coordinates are inaccurate.

### 5. Outputs and Included Data Files

#### Raw Data

- `data/raw/ev_20251216.csv`: The original December 2025 NSW charger dataset.
- `data/raw/SA4_2026_AUST_SHP_GDA2020.zip`: The original 2026 ABS SA4 boundary archive.

#### Processed Data

- `data/processed/chargers_clean.csv`: Contains 1,958 cleaned charger records, standardized fields, and quality review flags.
- `data/processed/charger_sa4.csv`: Contains SA4 assignments for all charger records. Unmatched records have no assigned region.
- `data/processed/charger_sa4_review.csv`: Contains candidate regions for unresolved spatial matches. These are not confirmed assignments.
- `data/processed/charger_attributes.csv`: Contains 216 accepted augmentation records from Open Charge Map, including additional attributes and matching information.
  
#### External Data Files

The following files are included in `data/external/`:

- `ocm_pilot_raw.json`: Cached Open Charge Map responses for the first 30 DC records.
- `ocm_australia_raw.json`: A cached Australian Open Charge Map dataset containing 1,367 records.
- `ocm_pilot_candidate_review.csv`: Candidate matches and review decisions from the pilot stage.
- `ocm_full_candidate_review.csv`: Candidate matches, matching scores, and review decisions for the full augmentation stage.

#### Database

- `data/final/project.duckdb`: The final DuckDB database containing charger records, operators, SA4 regions, spatial assignments, augmentation attributes, and validation metadata.
- `sql/schema.sql`: Defines the database tables, constraints, and views.
