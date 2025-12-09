-- ClickHouse: route-hour performance (last 7 days)
SELECT
  route_code,
  route_name,
  route_type,
  start_hour,
  trips_total,
  trips_completed,
  trips_canceled,
  round(avg_trip_duration_sec, 1) AS avg_trip_duration_sec,
  round(avg_distance_m, 1)        AS avg_distance_m,
  round(avg_fare_amount, 2)       AS avg_fare_amount
FROM mart.trip_route_hourly
WHERE start_hour >= now() - INTERVAL 7 DAY
ORDER BY start_hour DESC, route_code
LIMIT 200;

-- ClickHouse: peak-hour profile by route type (share of trips)
WITH base AS (
  SELECT route_type, toHour(start_hour) AS hour, sum(trips_total) AS trips
  FROM mart.trip_route_hourly
  WHERE start_hour >= now() - INTERVAL 7 DAY
  GROUP BY route_type, hour
)
SELECT
  route_type,
  hour,
  trips,
  round(trips / sum(trips) OVER (PARTITION BY route_type), 3) AS share_in_type
FROM base
ORDER BY route_type, hour;

-- ClickHouse: week-over-week revenue and cancellation trend
SELECT
  route_code,
  toStartOfWeek(start_hour) AS week_start,
  sum(total_fare_amount)    AS fare_sum,
  sum(trips_canceled)       AS canceled,
  sum(trips_total)          AS total,
  round(canceled / nullIf(total, 0), 3) AS cancel_rate
FROM mart.trip_route_hourly
WHERE start_hour >= now() - INTERVAL 56 DAY
GROUP BY route_code, week_start
ORDER BY week_start DESC, fare_sum DESC;

-- Postgres DWH: route-hour performance (last 7 days)
WITH trip_enriched AS (
  SELECT
    r.route_code,
    r.route_name,
    r.route_type,
    (d.full_date::timestamp + t.full_time)                   AS start_ts,
    date_trunc('hour', d.full_date::timestamp + t.full_time) AS start_hour,
    f.trip_status,
    f.trip_duration_sec,
    f.distance_meters,
    f.fare_amount
  FROM dwh.fact_trip f
  JOIN dwh.dim_route r ON r.route_sk = f.route_sk AND r.is_current = TRUE
  JOIN dwh.dim_date d  ON d.date_key = f.start_date_key
  JOIN dwh.dim_time t  ON t.time_key = f.start_time_key
  WHERE (d.full_date::timestamp + t.full_time) >= now() - interval '7 day'
)
SELECT
  route_code,
  route_name,
  route_type,
  start_hour,
  count(*)                                                    AS trips_total,
  sum((trip_status = 'completed')::int)                       AS trips_completed,
  sum((trip_status = 'canceled')::int)                        AS trips_canceled,
  round(avg(trip_duration_sec)::numeric, 1)                   AS avg_trip_duration_sec,
  round(avg(distance_meters)::numeric, 1)                     AS avg_distance_m,
  round(avg(fare_amount)::numeric, 2)                         AS avg_fare_amount
FROM trip_enriched
GROUP BY route_code, route_name, route_type, start_hour
ORDER BY start_hour DESC, route_code
LIMIT 200;

-- Postgres DWH: peak-hour profile by route type (share of trips)
WITH base AS (
  SELECT
    r.route_type,
    t.hour,
    count(*) AS trips
  FROM dwh.fact_trip f
  JOIN dwh.dim_route r ON r.route_sk = f.route_sk AND r.is_current = TRUE
  JOIN dwh.dim_date d  ON d.date_key = f.start_date_key
  JOIN dwh.dim_time t  ON t.time_key = f.start_time_key
  WHERE (d.full_date::timestamp + t.full_time) >= now() - interval '7 day'
  GROUP BY r.route_type, t.hour
)
SELECT
  route_type,
  hour,
  trips,
  round(trips::numeric / NULLIF(sum(trips) OVER (PARTITION BY route_type), 0), 3) AS share_in_type
FROM base
ORDER BY route_type, hour;

-- Postgres DWH: week-over-week revenue and cancellation trend
WITH trip_enriched AS (
  SELECT
    r.route_code,
    date_trunc('week', d.full_date::timestamp + t.full_time) AS week_start,
    f.trip_status,
    f.fare_amount
  FROM dwh.fact_trip f
  JOIN dwh.dim_route r ON r.route_sk = f.route_sk AND r.is_current = TRUE
  JOIN dwh.dim_date d  ON d.date_key = f.start_date_key
  JOIN dwh.dim_time t  ON t.time_key = f.start_time_key
  WHERE (d.full_date::timestamp + t.full_time) >= now() - interval '56 day'
)
SELECT
  route_code,
  week_start,
  sum(fare_amount)                            AS fare_sum,
  sum((trip_status = 'canceled')::int)        AS canceled,
  count(*)                                    AS total,
  round(sum((trip_status = 'canceled')::int)::numeric / NULLIF(count(*), 0), 3) AS cancel_rate
FROM trip_enriched
GROUP BY route_code, week_start
ORDER BY week_start DESC, fare_sum DESC;
