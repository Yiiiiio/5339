-- Standalone DDL: execute in a new database after INSTALL spatial; LOAD spatial.
-- Names retain upstream fields. Source records must not be labelled unique physical sites.
CREATE TABLE operators (operator_name VARCHAR PRIMARY KEY);
CREATE TABLE sa4_regions (
    SA4_CODE26 VARCHAR PRIMARY KEY,
    SA4_NAME26 VARCHAR NOT NULL,
    STE_CODE26 VARCHAR NOT NULL,
    STE_NAME26 VARCHAR NOT NULL,
    area_sq_km DOUBLE,
    geometry_crs VARCHAR NOT NULL CHECK (geometry_crs = 'EPSG:7844'),
    boundary_version VARCHAR NOT NULL,
    geom GEOMETRY
);
CREATE TABLE charger_records (
    "record_id" VARCHAR PRIMARY KEY,
    "OBJECTID" VARCHAR,
    "Station_name" VARCHAR,
    "Station_address" VARCHAR,
    "Operator" VARCHAR,
    "Number_of_plugs" INTEGER CHECK ("Number_of_plugs" > 0),
    "Charger_Type" VARCHAR,
    "Charger_rating" VARCHAR,
    "Latitude" DOUBLE NOT NULL CHECK (isfinite("Latitude") AND "Latitude" BETWEEN -90 AND 90),
    "Longitude" DOUBLE NOT NULL CHECK (isfinite("Longitude") AND "Longitude" BETWEEN -180 AND 180),
    "LGANAME" VARCHAR,
    "PCODE" VARCHAR,
    "Source" VARCHAR,
    "missing_region_metadata" BOOLEAN,
    "power_kw" DOUBLE CHECK (isfinite(power_kw) AND power_kw > 0),
    "power_format" VARCHAR,
    "operator_raw" VARCHAR,
    "operator_standardized" VARCHAR REFERENCES operators(operator_name),
    "operator_needs_review" BOOLEAN,
    "source_file" VARCHAR,
    "shared_coordinates" BOOLEAN,
    "shared_address_operator" BOOLEAN,
    "potential_duplicate_review" BOOLEAN,
    "postcode_from_address" VARCHAR,
    "postcode_check" VARCHAR,
    "postcode_standardized" VARCHAR,
    "postcode_source" VARCHAR,
    "postcode_needs_review" BOOLEAN,
    "postcode_resolution" VARCHAR,
    "charger_type_standardized" VARCHAR,
    "status_from_type" VARCHAR,
    geom GEOMETRY,
    geometry_crs VARCHAR NOT NULL DEFAULT 'EPSG:4326' CHECK (geometry_crs = 'EPSG:4326')
);
CREATE TABLE charger_sa4 (
    "record_id" VARCHAR PRIMARY KEY REFERENCES charger_records(record_id),
    "SA4_CODE26" VARCHAR REFERENCES sa4_regions(SA4_CODE26),
    "SA4_NAME26" VARCHAR,
    "STE_NAME26" VARCHAR,
    "sa4_match_status" VARCHAR,
    "charger_crs_assumed" VARCHAR,
    "charger_crs_status" VARCHAR,
    "sa4_boundary_crs" VARCHAR,
    "sa4_boundary_version" VARCHAR,
    CHECK ((SA4_CODE26 IS NULL AND sa4_match_status = 'unmatched') OR
           (SA4_CODE26 IS NOT NULL AND sa4_match_status = 'matched_within'))
);
CREATE SEQUENCE spatial_review_sequence START 1;
CREATE TABLE spatial_review (
    review_id BIGINT PRIMARY KEY DEFAULT nextval('spatial_review_sequence'),
    "record_id" VARCHAR NOT NULL REFERENCES charger_records(record_id),
    "Station_address" VARCHAR,
    "candidate_sa4_code" VARCHAR REFERENCES sa4_regions(SA4_CODE26),
    "candidate_sa4_name" VARCHAR,
    "candidate_distance_m" DOUBLE CHECK (isfinite(candidate_distance_m) AND candidate_distance_m >= 0),
    "review_status" VARCHAR,
    "charger_crs_status" VARCHAR,
    "distance_crs" VARCHAR
);
CREATE TABLE charger_attributes (
    "record_id" VARCHAR NOT NULL REFERENCES charger_records(record_id),
    "external_source" VARCHAR NOT NULL,
    "external_station_id" VARCHAR NOT NULL,
    "connector_types" VARCHAR,
    "external_power_kw_values" VARCHAR,
    "external_connection_quantity" INTEGER,
    "external_number_of_points" INTEGER,
    "external_status" VARCHAR,
    "external_usage_type" VARCHAR,
    "external_usage_cost" VARCHAR,
    "external_last_verified" TIMESTAMPTZ,
    "data_provider" VARCHAR,
    "match_distance_m" DOUBLE CHECK (isfinite(match_distance_m) AND match_distance_m >= 0),
    "match_score" INTEGER,
    "match_method" VARCHAR,
    "match_decision" VARCHAR NOT NULL CHECK (match_decision IN ('manual_accept', 'rule_based_accept')),
    "external_station_title" VARCHAR,
    "external_address" VARCHAR,
    PRIMARY KEY (record_id, external_source, external_station_id)
);
CREATE TABLE attribute_connectors (
    record_id VARCHAR,
    external_source VARCHAR,
    external_station_id VARCHAR,
    connector_type VARCHAR,
    PRIMARY KEY (record_id, external_source, external_station_id, connector_type),
    FOREIGN KEY (record_id, external_source, external_station_id)
        REFERENCES charger_attributes(record_id, external_source, external_station_id)
);
CREATE TABLE build_inputs (
    relative_path VARCHAR PRIMARY KEY,
    sha256 VARCHAR NOT NULL,
    size_bytes BIGINT NOT NULL,
    imported_at_utc TIMESTAMPTZ NOT NULL
);
CREATE TABLE build_metadata (key VARCHAR PRIMARY KEY, value JSON NOT NULL);

-- Safe one-row-per-source-record view: augmentation uses EXISTS rather than a fan-out join.
CREATE VIEW charger_overview AS
SELECT c.*, b.SA4_CODE26, b.SA4_NAME26, b.STE_NAME26, b.sa4_match_status,
       EXISTS (SELECT 1 FROM charger_attributes a WHERE a.record_id=c.record_id) AS has_augmentation
FROM charger_records c JOIN charger_sa4 b USING (record_id);

CREATE VIEW sa4_summary AS
SELECT r.SA4_CODE26, r.SA4_NAME26, r.STE_NAME26,
       count(c.record_id) AS source_records,
       count(c.record_id) FILTER (WHERE c.charger_type_standardized='DC') AS dc_source_records,
       count(c.record_id) FILTER (WHERE c.has_augmentation) AS augmented_source_records
FROM sa4_regions r LEFT JOIN charger_overview c USING (SA4_CODE26)
GROUP BY r.SA4_CODE26, r.SA4_NAME26, r.STE_NAME26;

CREATE VIEW augmentation_review AS
SELECT a.record_id, c.Station_address, a.external_station_id, a.external_station_title,
       a.external_address, a.match_distance_m, a.match_score, a.match_method,
       a.match_decision, a.external_last_verified
FROM charger_attributes a JOIN charger_records c USING (record_id)
WHERE a.match_distance_m > 3000;

CREATE VIEW reused_external_ids AS
SELECT external_source, external_station_id, count(DISTINCT record_id) AS source_record_count
FROM charger_attributes GROUP BY external_source, external_station_id
HAVING count(DISTINCT record_id)>1;
