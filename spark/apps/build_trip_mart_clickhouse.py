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
    dim_date = read_postgres_table(spark, "dwh.dim_date", jdbc_props) \
        .select("date_key", "full_date")
    dim_time = read_postgres_table(spark, "dwh.dim_time", jdbc_props) \
        .select("time_key", "full_time", "hour")

    enriched = (
        fact_trip.alias("f")
        .join(dim_route.alias("r"), "route_sk")
        .join(dim_date.alias("d"), F.col("f.start_date_key") == F.col("d.date_key"), "left")
        .join(dim_time.alias("t"), F.col("f.start_time_key") == F.col("t.time_key"), "left")
        .withColumn(
            "start_ts",
            F.to_timestamp(
                F.concat_ws(" ", F.col("d.full_date").cast("string"), F.col("t.full_time").cast("string"))
            ),
        )
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
        .filter("start_hour IS NOT NULL")
    )

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

    (
        df.write
        .mode(mode)
        .option("truncate", "true")
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
        print("[build_trip_mart_clickhouse] No trip data available for mart load")
        spark.stop()
        return

    save_mode = "overwrite" if args.reload_all else "append"
    write_to_clickhouse(mart_df, save_mode)

    spark.stop()


if __name__ == "__main__":
    main()
