from pyspark.sql import SparkSession, functions as F
import argparse
import config


def get_spark():
    spark = (
        SparkSession.builder
        .appName("metropulse_staging_load")
        .getOrCreate()
    )
    return spark


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--s3-base", default="s3a://metropulse-raw")
    parser.add_argument(
        "--jdbc-url", default="jdbc:postgresql://postgres:5432/metropulse_dwh")
    parser.add_argument("--db-user", default="metropulse")
    parser.add_argument("--db-password", default="metropulse")
    return parser.parse_args()


def add_common_etl_columns(df, batch_id: str, default_src_system):
    if default_src_system is not None and "src_system" not in df.columns:
        df = df.withColumn("src_system", F.lit(default_src_system))

    df = df.withColumn("etl_batch_id", F.lit(batch_id))
    df = df.withColumn("etl_loaded_at", F.current_timestamp())
    return df


def write_to_jdbc(df, table_name: str, jdbc_url: str, jdbc_props: dict):
    (
        df.write
        .mode("append")
        .jdbc(jdbc_url, table_name, properties=jdbc_props)
    )


def load_users(spark, args, jdbc_props):
    path = f"{args.s3_base}/users"
    print(f"[staging_load] reading {path}")

    df = spark.read.parquet(path)

    df = add_common_etl_columns(df, args.batch_id, default_src_system="oltp")

    if "src_created_at" not in df.columns:
        df = df.withColumn("src_created_at", F.lit(None).cast("timestamp"))
    if "src_updated_at" not in df.columns:
        df = df.withColumn("src_updated_at", F.lit(None).cast("timestamp"))

    if "registration_ts" in df.columns:
        df = df.withColumn("registration_ts", F.col(
            "registration_ts").cast("timestamp"))

    required_cols = [
        "user_id",
        "email",
        "phone",
        "full_name",
        "registration_ts",
        "status",
        "src_system",
        "src_created_at",
        "src_updated_at",
        "etl_batch_id",
        "etl_loaded_at",
    ]

    for c in required_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    df_out = df.select(*required_cols)

    print(f"[staging_load] writing {df_out.count()} rows to staging.stg_user")
    write_to_jdbc(df_out, "staging.stg_user", config.POSTGRES_JDBC_URL, jdbc_props)


def load_routes(spark, args, jdbc_props):
    path = f"{args.s3_base}/routes"
    print(f"[staging_load] reading {path}")

    df = spark.read.parquet(path)

    df = add_common_etl_columns(df, args.batch_id, default_src_system="oltp")

    if "src_created_at" not in df.columns:
        df = df.withColumn("src_created_at", F.lit(None).cast("timestamp"))
    if "src_updated_at" not in df.columns:
        df = df.withColumn("src_updated_at", F.lit(None).cast("timestamp"))

    if "valid_from_ts" in df.columns:
        df = df.withColumn("valid_from_ts", F.col(
            "valid_from_ts").cast("timestamp"))
    if "valid_to_ts" in df.columns:
        df = df.withColumn("valid_to_ts", F.col(
            "valid_to_ts").cast("timestamp"))
    if "is_active" in df.columns:
        df = df.withColumn("is_active", F.col("is_active").cast("boolean"))

    required_cols = [
        "route_id",
        "route_code",
        "route_name",
        "route_type",
        "description",
        "route_pattern_hash",
        "default_fare_product_id",
        "valid_from_ts",
        "valid_to_ts",
        "is_active",
        "src_system",
        "src_created_at",
        "src_updated_at",
        "etl_batch_id",
        "etl_loaded_at",
    ]

    for c in required_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    df_out = df.select(*required_cols)

    print(
        f"[staging_load] writing {df_out.count()} rows to staging.stg_route")
    write_to_jdbc(df_out, "staging.stg_route", config.POSTGRES_JDBC_URL, jdbc_props)


def load_trips(spark, args, jdbc_props):
    path = f"{args.s3_base}/trips"
    print(f"[staging_load] reading {path}")

    df = spark.read.parquet(path)

    df = add_common_etl_columns(df, args.batch_id, default_src_system="oltp")

    for col_name in ["start_ts", "end_ts", "created_at", "updated_at"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("timestamp"))

    required_cols = [
        "trip_id",
        "user_id",
        "route_id",
        "vehicle_id",
        "origin_stop_id",
        "destination_stop_id",
        "start_ts",
        "end_ts",
        "distance_meters",
        "fare_product_id",
        "fare_amount",
        "currency_code",
        "trip_status",
        "created_at",
        "updated_at",
        "src_system",
        "etl_batch_id",
        "etl_loaded_at",
    ]

    for c in required_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    df_out = df.select(*required_cols)

    print(
        f"[staging_load] writing {df_out.count()} rows to staging.stg_trip")
    write_to_jdbc(df_out, "staging.stg_trip", config.POSTGRES_JDBC_URL, jdbc_props)


def load_payments(spark, args, jdbc_props):
    path = f"{args.s3_base}/payments"
    print(f"[staging_load] reading {path}")

    df = spark.read.parquet(path)

    df = add_common_etl_columns(df, args.batch_id, default_src_system="oltp")

    for col_name in ["created_at", "updated_at"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("timestamp"))

    required_cols = [
        "payment_id",
        "user_id",
        "fare_product_id",
        "payment_method",
        "amount",
        "currency_code",
        "payment_status",
        "external_txn_id",
        "created_at",
        "updated_at",
        "src_system",
        "etl_batch_id",
        "etl_loaded_at",
    ]

    for c in required_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    df_out = df.select(*required_cols)

    print(
        f"[staging_load] writing {df_out.count()} rows to staging.stg_payment")
    write_to_jdbc(df_out, "staging.stg_payment", config.POSTGRES_JDBC_URL, jdbc_props)


def load_stops(spark, args, jdbc_props):
    path = f"{args.s3_base}/stops"
    print(f"[staging_load] reading {path}")

    df = spark.read.parquet(path)

    df = add_common_etl_columns(df, args.batch_id, default_src_system="oltp")

    if "src_created_at" not in df.columns:
        df = df.withColumn("src_created_at", F.lit(None).cast("timestamp"))
    if "src_updated_at" not in df.columns:
        df = df.withColumn("src_updated_at", F.lit(None).cast("timestamp"))

    for col_name in ["latitude", "longitude"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))

    required_cols = [
        "stop_id",
        "stop_code",
        "stop_name",
        "city",
        "latitude",
        "longitude",
        "zone",
        "src_system",
        "src_created_at",
        "src_updated_at",
        "etl_batch_id",
        "etl_loaded_at",
    ]

    for c in required_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    df_out = df.select(*required_cols)

    print(
        f"[staging_load] writing {df_out.count()} rows to staging.stg_stop")
    write_to_jdbc(df_out, "staging.stg_stop", config.POSTGRES_JDBC_URL, jdbc_props)


def load_vehicles(spark, args, jdbc_props):
    path = f"{args.s3_base}/vehicles"
    print(f"[staging_load] reading {path}")

    df = spark.read.parquet(path)

    df = add_common_etl_columns(
        df, args.batch_id, default_src_system="oltp_or_stream")

    if "src_created_at" not in df.columns:
        df = df.withColumn("src_created_at", F.lit(None).cast("timestamp"))
    if "src_updated_at" not in df.columns:
        df = df.withColumn("src_updated_at", F.lit(None).cast("timestamp"))

    if "capacity" in df.columns:
        df = df.withColumn("capacity", F.col("capacity").cast("int"))

    required_cols = [
        "vehicle_id",
        "registration_number",
        "vehicle_type",
        "capacity",
        "operator_name",
        "src_system",
        "src_created_at",
        "src_updated_at",
        "etl_batch_id",
        "etl_loaded_at",
    ]

    for c in required_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    df_out = df.select(*required_cols)

    print(
        f"[staging_load] writing {df_out.count()} rows to staging.stg_vehicle")
    write_to_jdbc(df_out, "staging.stg_vehicle", config.POSTGRES_JDBC_URL, jdbc_props)


def load_fare_products(spark, args, jdbc_props):
    path = f"{args.s3_base}/fare_products"
    print(f"[staging_load] reading {path}")

    df = spark.read.parquet(path)

    df = add_common_etl_columns(df, args.batch_id, default_src_system="oltp")

    if "src_created_at" not in df.columns:
        df = df.withColumn("src_created_at", F.lit(None).cast("timestamp"))
    if "src_updated_at" not in df.columns:
        df = df.withColumn("src_updated_at", F.lit(None).cast("timestamp"))

    if "base_price" in df.columns:
        df = df.withColumn("base_price", F.col(
            "base_price").cast("decimal(10,2)"))

    required_cols = [
        "fare_product_id",
        "product_name",
        "fare_type",
        "currency_code",
        "base_price",
        "description",
        "src_system",
        "src_created_at",
        "src_updated_at",
        "etl_batch_id",
        "etl_loaded_at",
    ]

    for c in required_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    df_out = df.select(*required_cols)

    print(
        f"[staging_load] writing {df_out.count()} rows to staging.stg_fare_product")
    write_to_jdbc(df_out, "staging.stg_fare_product",
                  config.POSTGRES_JDBC_URL, jdbc_props)


def load_vehicle_positions(spark, args, jdbc_props):
    """
    staging.stg_vehicle_position:
      raw_event_id, vehicle_id, route_id,
      event_ts, latitude, longitude,
      speed_kmph, heading_deg,
      nearest_stop_id,
      partition_id, offset_in_partition,
      src_system, etl_batch_id, etl_loaded_at
    """
    path = f"{args.s3_base}/vehicle_positions"
    print(f"[staging_load] reading {path}")

    df = spark.read.parquet(path)

    df = add_common_etl_columns(df, args.batch_id,
                                default_src_system="kafka_vehicle_positions")

    if "event_ts" in df.columns:
        df = df.withColumn("event_ts", F.col("event_ts").cast("timestamp"))

    for col_name in ["latitude", "longitude", "speed_kmph", "heading_deg"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))

    for col_name in ["partition_id"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("int"))
    for col_name in ["offset_in_partition"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("bigint"))

    required_cols = [
        "raw_event_id",
        "vehicle_id",
        "route_id",
        "event_ts",
        "latitude",
        "longitude",
        "speed_kmph",
        "heading_deg",
        "nearest_stop_id",
        "partition_id",
        "offset_in_partition",
        "src_system",
        "etl_batch_id",
        "etl_loaded_at",
    ]

    for c in required_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    df_out = df.select(*required_cols)

    print(
        f"[staging_load] writing {df_out.count()} rows to staging.stg_vehicle_position")
    write_to_jdbc(df_out, "staging.stg_vehicle_position",
                  config.POSTGRES_JDBC_URL, jdbc_props)


def main():
    args = parse_args()
    spark = get_spark()

    jdbc_props = {
        "user": config.POSTGRES_USER,
        "password": config.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    load_users(spark, args, jdbc_props)
    load_fare_products(spark, args, jdbc_props)
    load_stops(spark, args, jdbc_props)
    load_vehicles(spark, args, jdbc_props)
    load_routes(spark, args, jdbc_props)
    load_trips(spark, args, jdbc_props)
    load_payments(spark, args, jdbc_props)
    load_vehicle_positions(spark, args, jdbc_props)

    spark.stop()
    print("[staging_load] Done")


if __name__ == "__main__":
    main()
