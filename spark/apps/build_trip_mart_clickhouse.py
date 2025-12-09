import argparse

from pyspark.sql import SparkSession, functions as F

import config


def get_spark():
    return (
        SparkSession.builder
        .appName("metropulse_trip_route_hour_mart")
        .getOrCreate()
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build route-hour aggregates and load them into ClickHouse.")
    parser.add_argument(
        "--reload-all",
        action="store_true",
        help="If set, overwrite the mart table; otherwise append results.",
    )
    return parser.parse_args()


def read_postgres_table(spark, table_name: str, jdbc_props: dict):
    return (
        spark.read
        .format("jdbc")
        .option("url", config.POSTGRES_JDBC_URL)
        .option("dbtable", table_name)
        .option("user", config.POSTGRES_USER)
        .option("password", config.POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", 10000)
        .load()
    )


def build_route_hour_aggregates(spark, jdbc_props):
    fact_trip = read_postgres_table(spark, "dwh.fact_trip", jdbc_props)
    dim_route = read_postgres_table(spark, "dwh.dim_route", jdbc_props) \
        .filter("is_current = TRUE") \
        .select("route_sk", "route_id_nat", "route_code", "route_name", "route_type")

    fact_cnt = fact_trip.count()
    print(f"[build_trip_mart_clickhouse] fact_trip rows read: {fact_cnt}")

    padded_time = F.lpad(F.col("start_time_key").cast("string"), 6, "0")
    start_ts_str = F.concat_ws(" ", F.col("start_date_key").cast("string"), padded_time)

    enriched_raw = (
        fact_trip.alias("f")
        .join(dim_route.alias("r"), "route_sk")
        .withColumn("start_ts", F.to_timestamp(start_ts_str, "yyyyMMdd HHmmss"))
        .withColumn("start_hour", F.date_trunc("hour", F.col("start_ts")))
        .select(
            "route_id_nat",
            "route_code",
            "route_name",
            "route_type",
            "start_hour",
            "trip_status",
            "trip_duration_sec",
            "distance_meters",
            "fare_amount",
        )
    )

    enriched_total = enriched_raw.count()
    enriched_null = enriched_raw.filter("start_hour IS NULL").count()
    print(f"[build_trip_mart_clickhouse] enriched rows total: {enriched_total}, null start_hour: {enriched_null}")

    enriched = enriched_raw.filter("start_hour IS NOT NULL")

    agg = (
        enriched
        .groupBy(
            "route_id_nat",
            "route_code",
            "route_name",
            "route_type",
            "start_hour",
        )
        .agg(
            F.count("*").alias("trips_total"),
            F.sum(F.when(F.col("trip_status") == F.lit("completed"), 1).otherwise(0)).alias("trips_completed"),
            F.sum(F.when(F.col("trip_status") == F.lit("canceled"), 1).otherwise(0)).alias("trips_canceled"),
            F.avg("trip_duration_sec").alias("avg_trip_duration_sec"),
            F.sum("distance_meters").alias("total_distance_m"),
            F.avg("distance_meters").alias("avg_distance_m"),
            F.sum("fare_amount").alias("total_fare_amount"),
            F.avg("fare_amount").alias("avg_fare_amount"),
        )
        .withColumn("etl_loaded_at", F.current_timestamp())
    )

    return agg


def write_to_clickhouse(df, mode: str):
    clickhouse_props = {
        "user": config.CLICKHOUSE_USER,
        "password": config.CLICKHOUSE_PASSWORD,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    }
    create_opts = (
        "ENGINE = ReplacingMergeTree() "
        "PARTITION BY toDate(start_hour) "
        "ORDER BY (route_id_nat, start_hour)"
    )

    (
        df.write
        .mode(mode)
        .option("truncate", "true")
        .option("createTableOptions", create_opts)
        .jdbc(config.CLICKHOUSE_JDBC_URL, "mart.trip_route_hourly", properties=clickhouse_props)
    )


def main():
    args = parse_args()
    spark = get_spark()

    pg_props = {
        "user": config.POSTGRES_USER,
        "password": config.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    mart_df = build_route_hour_aggregates(spark, pg_props)

    if mart_df.rdd.isEmpty():
        total_trips = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.fact_trip", properties=pg_props).count()
        print(f"[build_trip_mart_clickhouse] No mart rows to write (fact_trip count={total_trips})")
        spark.stop()
        return

    mart_count = mart_df.count()
    print(f"[build_trip_mart_clickhouse] Rows to write: {mart_count}")

    save_mode = "overwrite" if args.reload_all else "append"
    write_to_clickhouse(mart_df, save_mode)

    ch_after = (
        spark.read
        .format("jdbc")
        .option("url", config.CLICKHOUSE_JDBC_URL)
        .option("dbtable", "mart.trip_route_hourly")
        .option("user", config.CLICKHOUSE_USER)
        .option("password", config.CLICKHOUSE_PASSWORD)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .load()
        .count()
    )
    print(f"[build_trip_mart_clickhouse] ClickHouse mart.trip_route_hourly row count after write: {ch_after}")

    spark.stop()


if __name__ == "__main__":
    main()
