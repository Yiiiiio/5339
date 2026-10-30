# Charging Dashboard

The dashboard uses Dash, dash-leaflet and Plotly to display station prices, connector occupancy and charging activity. It reads received events from DuckDB and refreshes once per second. A1 supplies coordinates, addresses, operators and available plug types; generated station metadata supplies display labels and simulated connector counts.

## Run

From the repository root, install `assignment2/requirements.txt` and run:

```bash
python assignment2/scripts/run_dashboard.py
```

The launcher creates `assignment2/data/runtime/replay.duckdb`, starts a local MQTT broker and the dashboard, and opens http://127.0.0.1:8050. Press Enter to publish the event file. Ctrl+C stops the services. Each launch resets the runtime database; `assignment2/data/final/project_a2.duckdb` retains the submitted results.

To inspect the submitted results without MQTT:

```bash
python assignment2/dashboard/app.py
```

## Display and interactions

- A marker appears when the station's first event is stored. Its label displays the latest price or occupied connectors, according to the selected mode.
- Clicking a marker shows the station name, address, operator, available plug types, price and occupancy. Missing source attributes are identified rather than inferred.
- An active session's popup contains a live cumulative-energy curve and its latest energy, power and state of charge. With multiple active sessions, the popup displays the most recently started session.
- The side panel provides a session selector, charging curves, historical prices, completed-session metrics and recent events.
- An active session has a received start and no received end. A vehicle that has stopped charging but remains plugged in still occupies its connector.

Prices and session state are derived only from received events. Initial empty states remain visible until the corresponding data arrives. Meter energy is cumulative within a session and is not summed across readings.

## Storage access

The dashboard and subscriber share a persistent DuckDB connection protected by a lock. The refresh timer pauses while a refresh is in progress. Shutdown stops the subscriber and checkpoints the database. Do not run a standalone writer against a database currently open in a separate dashboard process.

## Validation

```bash
python -m unittest discover -s assignment2/tests -v
```

The tests cover initial empty states, marker creation, current and historical metrics, charging curves, duplicate ingestion and concurrent reads during event writes. The submitted replay contains 1,482 events and yields prices for 433 stations, 19 active sessions, 6 completed sessions and 585 meter readings. See `../data/events/replay_validation.json` for the recorded transport validation.
