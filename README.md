# metro-pulse

# UI

## pgAdmin

[UI](http://localhost:5050)

`admin@admin.ad:admin`

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
