-- COMP5339 Assignment 2 - Role C database schema (price stage)
-- Run this file against a copy of the A1 DuckDB database.
-- The existing charger_records table is kept as the source of station details.

CREATE TABLE IF NOT EXISTS price_events (
    event_id VARCHAR PRIMARY KEY,
    event_type VARCHAR NOT NULL DEFAULT 'price_update'
        CHECK (event_type = 'price_update'),
    station_id VARCHAR NOT NULL
        REFERENCES charger_records(record_id),
    event_ts TIMESTAMP NOT NULL,
    price_per_kwh DECIMAL(8, 4) NOT NULL
        CHECK (price_per_kwh >= 0),
    wholesale_per_kwh DECIMAL(12, 6),
    received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    raw_payload JSON,
    UNIQUE (station_id, event_ts)
);

-- The latest price is derived from history instead of being copied into a
-- second table. A late older event can be stored without replacing a newer one.
CREATE OR REPLACE VIEW latest_station_price AS
SELECT
    event_id,
    station_id,
    event_ts,
    price_per_kwh,
    wholesale_per_kwh,
    received_at
FROM price_events
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY station_id
    ORDER BY event_ts DESC, received_at DESC, event_id DESC
) = 1;

-- Session stage: session_start / session_end and meter events.
-- Received events are stored as history; session state and station metrics
-- are views over that history, so a duplicate event can never be counted
-- twice and an end received before its start still yields one session.

CREATE TABLE IF NOT EXISTS session_events (
    event_id VARCHAR PRIMARY KEY,
    event_type VARCHAR NOT NULL
        CHECK (event_type IN ('session_start', 'session_end')),
    session_id VARCHAR NOT NULL,
    station_id VARCHAR NOT NULL
        REFERENCES charger_records(record_id),
    connector_id INTEGER NOT NULL CHECK (connector_id > 0),
    event_ts TIMESTAMP NOT NULL,
    -- Known at session_start.
    vehicle_model VARCHAR,
    price_per_kwh DECIMAL(8, 4) CHECK (price_per_kwh >= 0),
    -- Known only at session_end.
    charge_end_ts TIMESTAMP,
    charge_minutes DOUBLE CHECK (charge_minutes >= 0),
    energy_kwh DECIMAL(10, 3) CHECK (energy_kwh >= 0),
    avg_kw DOUBLE CHECK (avg_kw >= 0),
    peak_kw DOUBLE CHECK (peak_kw >= 0),
    cost_aud DECIMAL(10, 2) CHECK (cost_aud >= 0),
    received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    raw_payload JSON,
    UNIQUE (session_id, event_type)
);

CREATE TABLE IF NOT EXISTS meter_readings (
    event_id VARCHAR PRIMARY KEY,
    session_id VARCHAR NOT NULL,
    station_id VARCHAR NOT NULL
        REFERENCES charger_records(record_id),
    connector_id INTEGER NOT NULL CHECK (connector_id > 0),
    event_ts TIMESTAMP NOT NULL,
    voltage_v DOUBLE NOT NULL CHECK (voltage_v >= 0),
    current_a DOUBLE NOT NULL CHECK (current_a >= 0),
    power_kw DOUBLE NOT NULL CHECK (power_kw >= 0),
    -- Cumulative energy of the session so far; never sum across readings.
    energy_kwh DOUBLE NOT NULL CHECK (energy_kwh >= 0),
    soc_pct DOUBLE NOT NULL CHECK (soc_pct BETWEEN 0 AND 100),
    received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    raw_payload JSON,
    UNIQUE (session_id, event_ts)
);

-- No secondary index on session_id: DuckDB failed to delete rows from that
-- non-unique index when a demo copy was cleared. Drop it from older copies.
DROP INDEX IF EXISTS meter_readings_session_idx;

-- One row per session seen in session_events. 'active' means a start has been
-- received and no end yet: the connector is occupied (charging, or finished
-- charging but not yet unplugged).
CREATE OR REPLACE VIEW session_state AS
SELECT
    session_id,
    any_value(station_id) AS station_id,
    any_value(connector_id) AS connector_id,
    max(event_ts) FILTER (WHERE event_type = 'session_start') AS start_ts,
    max(vehicle_model) FILTER (WHERE event_type = 'session_start') AS vehicle_model,
    max(price_per_kwh) FILTER (WHERE event_type = 'session_start') AS start_price_per_kwh,
    max(event_ts) FILTER (WHERE event_type = 'session_end') AS end_ts,
    max(charge_end_ts) AS charge_end_ts,
    max(charge_minutes) AS charge_minutes,
    max(energy_kwh) AS energy_kwh,
    max(cost_aud) AS cost_aud,
    CASE
        WHEN count(*) FILTER (WHERE event_type = 'session_end') > 0 THEN 'completed'
        ELSE 'active'
    END AS status
FROM session_events
GROUP BY session_id;

-- Current and historical metrics per station, over all received events.
CREATE OR REPLACE VIEW station_session_metrics AS
SELECT
    station_id,
    count(*) FILTER (WHERE status = 'active') AS active_sessions,
    count(DISTINCT connector_id) FILTER (WHERE status = 'active') AS occupied_connectors,
    count(*) FILTER (WHERE status = 'completed') AS completed_sessions,
    coalesce(sum(energy_kwh) FILTER (WHERE status = 'completed'), 0) AS completed_energy_kwh,
    coalesce(sum(cost_aud) FILTER (WHERE status = 'completed'), 0) AS completed_revenue_aud
FROM session_state
GROUP BY station_id;
