#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

echo "[run.sh] Starting docker-compose stack..."
docker-compose up -d --build

echo "[run.sh] Waiting for Postgres to accept connections..."
ready="false"
for i in {1..30}; do
  if docker exec metropulse-postgres pg_isready -U metropulse >/dev/null 2>&1; then
    ready="true"
    break
  fi
  sleep 2
done
if [[ "$ready" != "true" ]]; then
  echo "[run.sh] Postgres not ready, exiting."
  exit 1
fi

echo "[run.sh] Waiting for ClickHouse to accept connections..."
ready="false"
for i in {1..30}; do
  if docker exec metropulse-clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
    ready="true"
    break
  fi
  sleep 2
done
if [[ "$ready" != "true" ]]; then
  echo "[run.sh] ClickHouse not ready, exiting."
  exit 1
fi

echo "[run.sh] Generating synthetic data into MinIO..."
pushd data-gen >/dev/null
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 main.py
deactivate
popd >/dev/null

echo "[run.sh] Running Spark ETL + mart pipeline..."
./spark/run_pipeline.sh

echo "[run.sh] Done. You can query ClickHouse UI at http://localhost:5521"
