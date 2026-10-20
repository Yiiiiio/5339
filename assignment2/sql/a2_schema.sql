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
