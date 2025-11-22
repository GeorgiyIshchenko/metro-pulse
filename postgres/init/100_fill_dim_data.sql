INSERT INTO dwh.dim_date (
    date_key,
    full_date,
    year,
    quarter,
    month,
    month_name,
    week_of_year,
    day_of_month,
    day_of_week,
    day_name,
    is_weekend
)
SELECT
    to_char(d::date, 'YYYYMMDD')::int           AS date_key,
    d::date                                     AS full_date,
    EXTRACT(YEAR FROM d)::smallint              AS year,
    EXTRACT(QUARTER FROM d)::smallint           AS quarter,
    EXTRACT(MONTH FROM d)::smallint             AS month,
    to_char(d::date, 'TMMonth')                 AS month_name,
    EXTRACT(WEEK FROM d)::smallint              AS week_of_year,
    EXTRACT(DAY FROM d)::smallint               AS day_of_month,
    EXTRACT(ISODOW FROM d)::smallint           AS day_of_week, -- 1=Mon .. 7=Sun
    to_char(d::date, 'TMDay')                   AS day_name,
    (EXTRACT(ISODOW FROM d) IN (6, 7))          AS is_weekend
FROM generate_series(
         DATE '2023-01-01',    
         DATE '2027-12-31',    
         INTERVAL '1 day'
     ) AS d
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO dwh.dim_time (
    time_key,
    full_time,
    hour,
    minute,
    second
)
SELECT
    (EXTRACT(HOUR   FROM t) * 10000 +
     EXTRACT(MINUTE FROM t) * 100 +
     EXTRACT(SECOND FROM t)
    )::int                         AS time_key,
    t::time                         AS full_time,
    EXTRACT(HOUR   FROM t)::smallint   AS hour,
    EXTRACT(MINUTE FROM t)::smallint   AS minute,
    EXTRACT(SECOND FROM t)::smallint   AS second
FROM generate_series(
         TIMESTAMP '2000-01-01 00:00:00',
         TIMESTAMP '2000-01-01 23:59:59',
         INTERVAL '1 second'
      ) AS t
ON CONFLICT (time_key) DO NOTHING;

