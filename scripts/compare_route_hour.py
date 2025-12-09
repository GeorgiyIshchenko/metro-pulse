#!/usr/bin/env python3
"""
Compare route-hour metrics between Postgres DWH and ClickHouse mart.
Outputs CSVs and a PNG chart into the chosen artifacts directory.
"""

import argparse
import json
import time
from pathlib import Path

import clickhouse_connect
import pandas as pd
from sqlalchemy import create_engine


def pg_route_hour_query(days: int, limit: int) -> str:
    return f"""
WITH trip_enriched AS (
  SELECT
    r.route_code,
    r.route_name,
    r.route_type,
    (d.full_date::timestamp + t.full_time)                       AS start_ts,
    date_trunc('hour', d.full_date::timestamp + t.full_time)     AS start_hour,
    f.trip_status,
    f.trip_duration_sec,
    f.distance_meters,
    f.fare_amount
  FROM dwh.fact_trip f
  JOIN dwh.dim_route r ON r.route_sk = f.route_sk AND r.is_current = TRUE
  JOIN dwh.dim_date d  ON d.date_key = f.start_date_key
  JOIN dwh.dim_time t  ON t.time_key = f.start_time_key
  WHERE (d.full_date::timestamp + t.full_time) >= now() - interval '{days} day'
)
SELECT
  route_code,
  route_name,
  route_type,
  start_hour,
  count(*)                                                    AS trips_total,
  sum((trip_status = 'completed')::int)                       AS trips_completed,
  sum((trip_status = 'canceled')::int)                        AS trips_canceled,
  round(avg(trip_duration_sec)::numeric, 2)                   AS avg_trip_duration_sec,
  round(avg(distance_meters)::numeric, 2)                     AS avg_distance_m,
  round(avg(fare_amount)::numeric, 2)                         AS avg_fare_amount
FROM trip_enriched
GROUP BY route_code, route_name, route_type, start_hour
ORDER BY start_hour DESC, route_code
LIMIT {limit};
"""


def ch_route_hour_query(days: int, limit: int) -> str:
    return f"""
SELECT
  route_code,
  route_name,
  route_type,
  start_hour,
  trips_total,
  trips_completed,
  trips_canceled,
  round(avg_trip_duration_sec, 2) AS avg_trip_duration_sec,
  round(avg_distance_m, 2)        AS avg_distance_m,
  round(avg_fare_amount, 2)       AS avg_fare_amount
FROM mart.trip_route_hourly
WHERE start_hour >= now() - INTERVAL {days} DAY
ORDER BY start_hour DESC, route_code
LIMIT {limit}
"""


def fetch_postgres(pg_url: str, query: str) -> pd.DataFrame:
    engine = create_engine(pg_url)
    df = pd.read_sql(query, engine, parse_dates=["start_hour"])
    if "start_hour" in df.columns:
        df = df.copy()
        df.loc[:, "start_hour"] = pd.to_datetime(df["start_hour"])
    return df


def fetch_clickhouse(ch_host: str, ch_port: int, ch_user: str, ch_password: str, query: str) -> pd.DataFrame:
    empty_frame = pd.DataFrame(
        {
            "route_code": pd.Series(dtype="object"),
            "route_name": pd.Series(dtype="object"),
            "route_type": pd.Series(dtype="object"),
            "start_hour": pd.Series(dtype="datetime64[ns]"),
            "trips_total": pd.Series(dtype="int64"),
            "trips_completed": pd.Series(dtype="int64"),
            "trips_canceled": pd.Series(dtype="int64"),
            "avg_trip_duration_sec": pd.Series(dtype="float64"),
            "avg_distance_m": pd.Series(dtype="float64"),
            "avg_fare_amount": pd.Series(dtype="float64"),
        }
    )
    client = clickhouse_connect.get_client(
        host=ch_host,
        port=ch_port,
        username=ch_user,
        password=ch_password,
        database="metropulse",
    )
    try:
        result = client.query(query)
    except Exception as exc:  # noqa: BLE001 - we want to catch driver errors and continue
        msg = str(exc)
        if "UNKNOWN_DATABASE" in msg or "UNKNOWN_TABLE" in msg or "does not exist" in msg:
            print(f"[clickhouse] {msg} — returning empty result (run pipeline to create mart table)")
            return empty_frame
        raise
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    if df.empty:
        return empty_frame
    if "start_hour" in df.columns:
        df = df.copy()
        df.loc[:, "start_hour"] = pd.to_datetime(df["start_hour"])
    return df


def ensure_datetime(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns:
        df = df.copy()
        df.loc[:, col] = pd.to_datetime(df[col], errors="coerce")
    return df


def ensure_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    if not cols:
        return df
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df.loc[:, c] = pd.to_numeric(df[c], errors="coerce")
    return df


def save_plot(df: pd.DataFrame, out_html: Path) -> None:
    if df.empty:
        print("[plot] Skipped plot (no data)")
        return
    hourly = (
        df.sort_values("start_hour")
        .groupby("start_hour")[["trips_total_pg", "trips_total_ch"]]
        .sum()
        .tail(48)
    )
    if hourly.empty:
        print("[plot] Skipped plot (no merged rows)")
        return
    try:
        import plotly.graph_objects as go
    except ImportError:
        print("[plot] plotly not installed; skipping plot")
        return
    hourly_plot = hourly.reset_index(drop=False).copy()
    hourly_plot.loc[:, "start_hour_str"] = hourly_plot["start_hour"].astype(str)

    fig = go.Figure()
    fig.add_bar(
        x=hourly_plot["start_hour_str"].tolist(),
        y=hourly_plot["trips_total_pg"].tolist(),
        name="Postgres",
    )
    fig.add_bar(
        x=hourly_plot["start_hour_str"].tolist(),
        y=hourly_plot["trips_total_ch"].tolist(),
        name="ClickHouse",
    )
    fig.update_layout(
        title="Trips per hour: Postgres vs ClickHouse (last 48 hours in window)",
        barmode="group",
        xaxis_title="start_hour",
        yaxis_title="trips_total",
        legend_title="source",
    )
    out_html.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(out_html)
    print(f"[plot] Saved {out_html}")


def save_route_diff_plots(df: pd.DataFrame, out_html: Path) -> None:
    if df.empty:
        print("[plot-route] Skipped plot (no merged data)")
        return
    # Aggregate to route level for a concise comparison
    agg = (
        df.groupby("route_code")
        .agg(
            trips_pg=("trips_total_pg", "sum"),
            trips_ch=("trips_total_ch", "sum"),
            fare_pg=("avg_fare_amount_pg", "mean"),
            fare_ch=("avg_fare_amount_ch", "mean"),
            duration_pg=("avg_trip_duration_sec_pg", "mean"),
            duration_ch=("avg_trip_duration_sec_ch", "mean"),
        )
        .reset_index()
    )
    if agg.empty:
        print("[plot-route] Skipped plot (no aggregated rows)")
        return

    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "Total trips by route",
            "Trips difference (PG - CH)",
            "Avg fare by route",
            "Avg duration by route",
        ),
    )

    fig.add_bar(
        row=1,
        col=1,
        x=agg["route_code"],
        y=agg["trips_pg"],
        name="Trips PG",
        marker_color="#1f77b4",
    )
    fig.add_bar(
        row=1,
        col=1,
        x=agg["route_code"],
        y=agg["trips_ch"],
        name="Trips CH",
        marker_color="#ff7f0e",
    )

    trips_diff = (agg["trips_pg"] - agg["trips_ch"]).tolist()
    fig.add_bar(
        row=1,
        col=2,
        x=agg["route_code"],
        y=trips_diff,
        name="Trips diff (PG-CH)",
        marker_color="#2ca02c",
    )

    fig.add_bar(
        row=2,
        col=1,
        x=agg["route_code"],
        y=agg["fare_pg"],
        name="Avg fare PG",
        marker_color="#17becf",
    )
    fig.add_bar(
        row=2,
        col=1,
        x=agg["route_code"],
        y=agg["fare_ch"],
        name="Avg fare CH",
        marker_color="#d62728",
    )

    fig.add_bar(
        row=2,
        col=2,
        x=agg["route_code"],
        y=agg["duration_pg"],
        name="Avg duration PG",
        marker_color="#8c564b",
    )
    fig.add_bar(
        row=2,
        col=2,
        x=agg["route_code"],
        y=agg["duration_ch"],
        name="Avg duration CH",
        marker_color="#9467bd",
    )

    fig.update_layout(
        title="Route-level comparison: Postgres vs ClickHouse",
        showlegend=True,
        height=800,
    )
    fig.update_xaxes(title_text="route_code", tickangle=45)
    fig.update_yaxes(title_text="count", row=1, col=1)
    fig.update_yaxes(title_text="diff", row=1, col=2)
    fig.update_yaxes(title_text="avg fare", row=2, col=1)
    fig.update_yaxes(title_text="avg duration (sec)", row=2, col=2)

    out_html.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(out_html)
    print(f"[plot-route] Saved {out_html}")


def save_performance_html(metrics: dict, out_html: Path) -> None:
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Performance metrics</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    table {{ border-collapse: collapse; min-width: 320px; }}
    th, td {{ border: 1px solid #ccc; padding: 8px 12px; text-align: left; }}
    th {{ background: #f5f5f5; }}
  </style>
</head>
<body>
  <h2>Query performance: Postgres vs ClickHouse</h2>
  <table>
    <tr><th>Metric</th><th>Value</th></tr>
    <tr><td>Postgres rows</td><td>{metrics.get("postgres_rows", "n/a")}</td></tr>
    <tr><td>ClickHouse rows</td><td>{metrics.get("clickhouse_rows", "n/a")}</td></tr>
    <tr><td>Postgres query seconds</td><td>{metrics.get("postgres_query_seconds", "n/a")}</td></tr>
    <tr><td>ClickHouse query seconds</td><td>{metrics.get("clickhouse_query_seconds", "n/a")}</td></tr>
  </table>
</body>
</html>
"""
    out_html.parent.mkdir(parents=True, exist_ok=True)
    out_html.write_text(html, encoding="utf-8")
    print(f"[metrics] Saved {out_html}")


def main():
    parser = argparse.ArgumentParser(description="Compare route-hour metrics in Postgres vs ClickHouse")
    parser.add_argument("--days", type=int, default=7, help="Lookback window in days")
    parser.add_argument("--limit", type=int, default=5000, help="Limit rows per source query")
    parser.add_argument("--pg-url", default="postgresql+psycopg://metropulse:metropulse@localhost:5432/metropulse_dwh",
                        help="SQLAlchemy Postgres URL (psycopg3 driver)")
    parser.add_argument("--ch-host", default="localhost", help="ClickHouse host")
    parser.add_argument("--ch-port", type=int, default=8123, help="ClickHouse HTTP port")
    parser.add_argument("--ch-user", default="metropulse", help="ClickHouse user")
    parser.add_argument("--ch-password", default="metropulse", help="ClickHouse password")
    parser.add_argument("--out-dir", default="artifacts", help="Directory to store outputs")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    pg_sql = pg_route_hour_query(args.days, args.limit)
    ch_sql = ch_route_hour_query(args.days, args.limit)

    print("[postgres] Running query...")
    t0 = time.perf_counter()
    df_pg = fetch_postgres(args.pg_url, pg_sql)
    pg_duration = time.perf_counter() - t0
    print(f"[postgres] Rows: {len(df_pg)} in {pg_duration:.3f}s")
    df_pg = ensure_datetime(df_pg, "start_hour")
    df_pg = ensure_numeric(
        df_pg,
        [
            "trips_total",
            "trips_completed",
            "trips_canceled",
            "avg_trip_duration_sec",
            "avg_distance_m",
            "avg_fare_amount",
        ],
    )

    print("[clickhouse] Running query...")
    t0 = time.perf_counter()
    df_ch = fetch_clickhouse(args.ch_host, args.ch_port, args.ch_user, args.ch_password, ch_sql)
    ch_duration = time.perf_counter() - t0
    print(f"[clickhouse] Rows: {len(df_ch)} in {ch_duration:.3f}s")
    df_ch = ensure_datetime(df_ch, "start_hour")
    df_ch = ensure_numeric(
        df_ch,
        [
            "trips_total",
            "trips_completed",
            "trips_canceled",
            "avg_trip_duration_sec",
            "avg_distance_m",
            "avg_fare_amount",
        ],
    )

    required_keys = {"route_code", "start_hour"}
    if not required_keys.issubset(df_pg.columns) or not required_keys.issubset(df_ch.columns):
        print("[merge] Missing route_code/start_hour in one of the datasets; skipping merge")
        merged = pd.DataFrame()
    else:
        merged = df_pg.merge(df_ch, on=["route_code", "start_hour"], suffixes=("_pg", "_ch"))
        if merged.empty:
            print("[merge] No overlapping route_code/start_hour rows")
        else:
            merged = ensure_numeric(
                merged,
                [
                    "trips_total_pg",
                    "trips_total_ch",
                    "avg_fare_amount_pg",
                    "avg_fare_amount_ch",
                    "avg_trip_duration_sec_pg",
                    "avg_trip_duration_sec_ch",
                ],
            )
            merged = merged.copy()
            merged.loc[:, "trips_diff"] = merged["trips_total_pg"] - merged["trips_total_ch"]
            merged.loc[:, "fare_diff"] = merged["avg_fare_amount_pg"] - merged["avg_fare_amount_ch"]
            merged.loc[:, "duration_diff"] = merged["avg_trip_duration_sec_pg"] - merged["avg_trip_duration_sec_ch"]

    df_pg.to_csv(out_dir / "route_hour_pg.csv", index=False)
    df_ch.to_csv(out_dir / "route_hour_ch.csv", index=False)
    merged.to_csv(out_dir / "route_hour_comparison.csv", index=False)
    print(f"[output] Saved CSVs to {out_dir}")

    save_plot(merged, out_dir / "trips_per_hour_comparison.html")
    save_route_diff_plots(merged, out_dir / "route_diff_comparison.html")

    metrics = {
        "postgres_query_seconds": pg_duration,
        "clickhouse_query_seconds": ch_duration,
        "postgres_rows": int(len(df_pg)),
        "clickhouse_rows": int(len(df_ch)),
    }
    with open(out_dir / "performance_metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    print(f"[metrics] Saved performance_metrics.json to {out_dir}")
    save_performance_html(metrics, out_dir / "performance_metrics.html")


if __name__ == "__main__":
    main()
