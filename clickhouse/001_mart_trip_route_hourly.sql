CREATE DATABASE IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.trip_route_hourly
(
    route_id_nat          String,
    route_code            String,
    route_name            String,
    route_type            String,
    start_hour            DateTime,

    trips_total           UInt64,
    trips_completed       UInt64,
    trips_canceled        UInt64,
    avg_trip_duration_sec Float64,
    total_distance_m      Float64,
    avg_distance_m        Float64,
    total_fare_amount     Float64,
    avg_fare_amount       Float64,

    etl_loaded_at         DateTime
)
ENGINE = ReplacingMergeTree()
PARTITION BY toDate(start_hour)
ORDER BY (route_id_nat, start_hour);
