CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.trips (
    ride_id VARCHAR(50),
    rideable_type VARCHAR(50),
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    start_station_name VARCHAR(255),
    start_station_id VARCHAR(50),
    end_station_name VARCHAR(255),
    end_station_id VARCHAR(50),
    start_lat NUMERIC,
    start_lng NUMERIC,
    end_lat NUMERIC,
    end_lng NUMERIC,
    member_casual VARCHAR(20),
    ingestion_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
