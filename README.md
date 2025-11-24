# metro-pulse

# UI

## pgAdmin

[UI](http://localhost:5050)

`admin@metropulse.local:admin`

## MinIO

[Console](http://localhost:9001)

`minio:minio123456`

## Spark

[Master](http://localhost:8080)
[Worker](http://localhost:8081)

## ClickHouse

[Frontend](http://localhost:5521)

# Running

To launch all services:
```bash
docker-compose up
```

To simulate some data flow:

Generate some data and insert it into MinIO:
```bash
cd data-gen
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 main.py
```


Run ETL pipeline:
```bash
./spark/run_pipeline.sh
```

# One-shot end-to-end run

```bash
./run.sh
```

What it does: builds/starts the stack, waits for Postgres and ClickHouse, generates synthetic data into MinIO, and runs the full Spark ETL + mart load. Open ClickHouse UI at http://localhost:5521 to query `mart.trip_route_hourly`.

# Batch mart to ClickHouse

1. Rebuild Spark images to pull the ClickHouse JDBC driver:
   ```bash
   docker-compose build spark-master spark-worker
   ```
2. Create the mart table in ClickHouse:
   ```bash
   docker exec -i metropulse-clickhouse clickhouse-client --user metropulse --password metropulse --multiquery < clickhouse/001_mart_trip_route_hourly.sql
   ```
3. (Re)load DWH + mart:
   ```bash
   ./spark/run_pipeline.sh
   ```
4. Example analytical query (fast on ClickHouse):
   ```sql
   SELECT route_code, start_hour, trips_total, avg_trip_duration_sec, total_fare_amount
   FROM mart.trip_route_hourly
   ORDER BY start_hour DESC, route_code
   LIMIT 10;
   ```
