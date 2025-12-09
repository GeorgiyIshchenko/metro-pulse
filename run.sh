#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

echo "[run.sh] Starting docker-compose stack..."
docker-compose up -d --build

function fail() {
  echo "[run.sh] $1"
  exit 1
}

function wait_for_pg() {
  echo "[run.sh] Waiting for Postgres to accept connections..."
  for _ in {1..30}; do
    if docker exec metropulse-postgres pg_isready -U metropulse >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

function wait_for_ch() {
  echo "[run.sh] Waiting for ClickHouse to accept connections..."
  for _ in {1..30}; do
    if docker exec metropulse-clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

function count_pg_fact_trip() {
  docker exec metropulse-postgres psql -U metropulse -d metropulse_dwh -t -A -c "SELECT count(*) FROM dwh.fact_trip;" 2>/dev/null
}

function count_pg_dim_route_current() {
  docker exec metropulse-postgres psql -U metropulse -d metropulse_dwh -t -A -c "SELECT count(*) FROM dwh.dim_route WHERE is_current = TRUE;" 2>/dev/null
}

function count_pg_fact_trip_route_null() {
  docker exec metropulse-postgres psql -U metropulse -d metropulse_dwh -t -A -c "SELECT count(*) FROM dwh.fact_trip WHERE route_sk IS NULL;" 2>/dev/null
}

function count_ch_mart() {
  docker exec metropulse-clickhouse clickhouse-client --query "SELECT count() FROM mart.trip_route_hourly;" 2>/dev/null
}

echo "[run.sh] Waiting for Postgres to accept connections..."
wait_for_pg || fail "Postgres not ready, exiting."

echo "[run.sh] Waiting for ClickHouse to accept connections..."
wait_for_ch || fail "ClickHouse not ready, exiting."

# Some tools may still point at a legacy DB name; create it to avoid noisy FATAL logs.
if ! docker exec metropulse-postgres psql -U metropulse -d postgres -t -A -c "SELECT 1 FROM pg_database WHERE datname='metropulse';" 2>/dev/null | grep -q 1; then
  echo "[run.sh] Creating auxiliary database 'metropulse' in Postgres (placeholder)..."
  docker exec metropulse-postgres createdb -U metropulse metropulse || true
fi

echo "[run.sh] Ensuring mart table exists in ClickHouse..."
docker exec -i metropulse-clickhouse clickhouse-client --user metropulse --password metropulse --multiquery < clickhouse/001_mart_trip_route_hourly.sql

# test / full
DATA_PROFILE="${PIPELINE_PROFILE:-full}"
echo "[run.sh] Generating synthetic data into MinIO..."
pushd data-gen >/dev/null
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 main.py --profile "$DATA_PROFILE"
deactivate
popd >/dev/null

PIPELINE_TIMEOUT_SECONDS="${PIPELINE_TIMEOUT_SECONDS:-900}"
echo "[run.sh] Running Spark ETL + mart pipeline (timeout ${PIPELINE_TIMEOUT_SECONDS}s)..."
run_with_timeout() {
  local t=$1; shift
  if command -v timeout >/dev/null 2>&1; then
    timeout "$t" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$t" "$@"
  else
    echo "[run.sh] 'timeout' command not found; running without timeout. Abort manually if it hangs."
    "$@"
    return $?
  fi
}
if run_with_timeout "$PIPELINE_TIMEOUT_SECONDS" ./spark/run_pipeline.sh; then
  echo "[run.sh] Spark pipeline finished"
else
  fail "Spark pipeline did not finish within ${PIPELINE_TIMEOUT_SECONDS}s; check spark logs"
fi

echo "[run.sh] Verifying data landed in Postgres..."
FACT_TRIP_COUNT="$(count_pg_fact_trip)"
if [[ -z "$FACT_TRIP_COUNT" || "$FACT_TRIP_COUNT" -le 0 ]]; then
  fail "Postgres fact_trip has no rows ($FACT_TRIP_COUNT). Check pipeline logs."
fi
echo "[run.sh] fact_trip rows: $FACT_TRIP_COUNT"
DIM_ROUTE_CURRENT="$(count_pg_dim_route_current)"
echo "[run.sh] dim_route current rows: ${DIM_ROUTE_CURRENT:-unknown}"
FACT_ROUTE_NULL="$(count_pg_fact_trip_route_null)"
echo "[run.sh] fact_trip route_sk NULL rows: ${FACT_ROUTE_NULL:-unknown}"

echo "[run.sh] Verifying mart table in ClickHouse..."
CH_MART_COUNT="$(count_ch_mart)"
if [[ -z "$CH_MART_COUNT" || "$CH_MART_COUNT" -le 0 ]]; then
  fail "ClickHouse mart.trip_route_hourly has no rows ($CH_MART_COUNT). Check pipeline logs."
fi
echo "[run.sh] mart.trip_route_hourly rows: $CH_MART_COUNT"

echo "[run.sh] Comparing Postgres DWH vs ClickHouse mart (route-hour)..."
VENV_SCRIPTS=".venv_scripts"
python3 -m venv "$VENV_SCRIPTS"
source "$VENV_SCRIPTS/bin/activate"
pip install -r scripts/requirements.txt
python3 scripts/compare_route_hour.py --out-dir artifacts
deactivate

echo "[run.sh] Done. You can query ClickHouse UI at http://localhost:5521"
