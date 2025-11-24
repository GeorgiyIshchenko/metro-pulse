from pyspark.sql import SparkSession, functions as F, Window
import argparse
import config


def get_spark():
    return (
        SparkSession.builder
        .appName("metropulse_dwh_load_facts")
        .getOrCreate()
    )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--batch-id",
        required=False,
        help="Batch ID to filter by",
    )
    return parser.parse_args()


def read_staging_table(spark, args, jdbc_props, table_name):
    df = spark.read.jdbc(config.POSTGRES_JDBC_URL, table_name, properties=jdbc_props)
    if args.batch_id:
        df = df.filter(F.col("etl_batch_id") == F.lit(args.batch_id))
    return df

def load_fact_trip(spark, args, jdbc_props):
    stg_trip = read_staging_table(spark, args, jdbc_props, "staging.stg_trip")

    dim_user = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_user", properties=jdbc_props) \
        .select("user_sk", "user_id_nat")

    dim_vehicle = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_vehicle", properties=jdbc_props) \
        .select("vehicle_sk", "vehicle_id_nat")

    dim_stop = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_stop", properties=jdbc_props) \
        .select("stop_sk", "stop_id_nat")

    dim_route = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_route", properties=jdbc_props) \
        .filter("is_current = TRUE") \
        .select("route_sk", "route_id_nat")

    dim_fare = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_fare_product", properties=jdbc_props) \
        .select("fare_product_sk", "fare_product_id_nat")

    trips_enriched = (
        stg_trip.alias("t")
        .join(dim_user.alias("u"), F.col("t.user_id") == F.col("u.user_id_nat"), "inner")
        .join(dim_route.alias("r"), F.col("t.route_id") == F.col("r.route_id_nat"), "left")
        .join(dim_vehicle.alias("v"), F.col("t.vehicle_id") == F.col("v.vehicle_id_nat"), "left")
        .join(dim_stop.alias("os"), F.col("t.origin_stop_id") == F.col("os.stop_id_nat"), "left")
        .join(dim_stop.alias("ds"), F.col("t.destination_stop_id") == F.col("ds.stop_id_nat"), "left")
        .join(dim_fare.alias("fp"), F.col("t.fare_product_id") == F.col("fp.fare_product_id_nat"), "left")
    )

    trips_enriched = (
        trips_enriched
        .withColumn("start_date_key", F.date_format("start_ts", "yyyyMMdd").cast("int"))
        .withColumn("start_time_key", F.date_format("start_ts", "HHmmss").cast("int"))
        .withColumn("end_date_key",   F.date_format("end_ts", "yyyyMMdd").cast("int"))
        .withColumn("end_time_key",   F.date_format("end_ts", "HHmmss").cast("int"))
        .withColumn(
            "trip_duration_sec",
            (F.col("end_ts").cast("long") - F.col("start_ts").cast("long")).cast("int")
        )
        .withColumn(
            "currency_code",
            F.coalesce(F.col("currency_code"), F.lit("EUR"))
        )
    )

    fact_trip = trips_enriched.select(
        F.monotonically_increasing_id().alias("trip_id"),
        F.col("u.user_sk").alias("user_sk"),
        F.col("r.route_sk").alias("route_sk"),
        F.col("v.vehicle_sk").alias("vehicle_sk"),
        F.col("os.stop_sk").alias("origin_stop_sk"),
        F.col("ds.stop_sk").alias("destination_stop_sk"),
        "start_date_key",
        "start_time_key",
        "end_date_key",
        "end_time_key",
        F.col("fp.fare_product_sk").alias("fare_product_sk"),
        "trip_duration_sec",
        "distance_meters",
        "fare_amount",
        "currency_code",
        "trip_status",
        "created_at",
        F.current_timestamp().alias("etl_loaded_at"),
    )

    fact_trip_filtered = fact_trip.filter(
        "user_sk IS NOT NULL AND start_date_key IS NOT NULL AND start_time_key IS NOT NULL"
    )

    if fact_trip_filtered.count() > 0:
        (
            fact_trip_filtered.write
            .mode("append")
            .jdbc(config.POSTGRES_JDBC_URL, "dwh.fact_trip", properties=jdbc_props)
        )


def load_fact_payment(spark, args, jdbc_props):
    stg_payment = read_staging_table(spark, args, jdbc_props, "staging.stg_payment")

    dim_user = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_user", properties=jdbc_props) \
        .select("user_sk", "user_id_nat")

    dim_fare = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_fare_product", properties=jdbc_props) \
        .select("fare_product_sk", "fare_product_id_nat")

    pay_enriched = (
        stg_payment.alias("p")
        .join(dim_user.alias("u"), F.col("p.user_id") == F.col("u.user_id_nat"), "inner")
        .join(dim_fare.alias("fp"), F.col("p.fare_product_id") == F.col("fp.fare_product_id_nat"), "left")
    )

    pay_enriched = (
        pay_enriched
        .withColumn("payment_date_key", F.date_format("created_at", "yyyyMMdd").cast("int"))
        .withColumn("payment_time_key", F.date_format("created_at", "HHmmss").cast("int"))
        .withColumn(
            "currency_code",
            F.coalesce(F.col("currency_code"), F.lit("EUR"))
        )
    )

    fact_payment = pay_enriched.select(
        F.monotonically_increasing_id().alias("payment_id"),
        F.col("u.user_sk").alias("user_sk"),
        F.col("fp.fare_product_sk").alias("fare_product_sk"),
        "payment_date_key",
        "payment_time_key",
        "payment_method",
        "amount",
        "currency_code",
        "payment_status",
        "external_txn_id",
        "created_at",
        F.current_timestamp().alias("etl_loaded_at"),
    )

    fact_payment_filtered = fact_payment.filter(
        "user_sk IS NOT NULL AND payment_date_key IS NOT NULL AND payment_time_key IS NOT NULL"
    )

    if fact_payment_filtered.count() > 0:
        (
            fact_payment_filtered.write
            .mode("append")
            .jdbc(config.POSTGRES_JDBC_URL, "dwh.fact_payment", properties=jdbc_props)
        )


def load_fact_vehicle_position(spark, args, jdbc_props):
    stg_vp = read_staging_table(spark, args, jdbc_props, "staging.stg_vehicle_position")

    dim_vehicle = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_vehicle", properties=jdbc_props) \
        .select("vehicle_sk", "vehicle_id_nat")

    dim_route = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_route", properties=jdbc_props) \
        .filter("is_current = TRUE") \
        .select("route_sk", "route_id_nat")

    dim_stop = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_stop", properties=jdbc_props) \
        .select("stop_sk", "stop_id_nat")

    vp_enriched = (
        stg_vp.alias("vp")
        .join(dim_vehicle.alias("v"), F.col("vp.vehicle_id") == F.col("v.vehicle_id_nat"), "inner")
        .join(dim_route.alias("r"), F.col("vp.route_id") == F.col("r.route_id_nat"), "left")
        .join(dim_stop.alias("s"), F.col("vp.nearest_stop_id") == F.col("s.stop_id_nat"), "left")
    )

    vp_enriched = (
        vp_enriched
        .withColumn("date_key", F.date_format("event_ts", "yyyyMMdd").cast("int"))
        .withColumn("time_key", F.date_format("event_ts", "HHmmss").cast("int"))
    )

    fact_vp = vp_enriched.select(
        F.monotonically_increasing_id().alias("vehicle_position_id"),
        F.col("v.vehicle_sk").alias("vehicle_sk"),
        F.col("r.route_sk").alias("route_sk"),
        "date_key",
        "time_key",
        "latitude",
        "longitude",
        "speed_kmph",
        "heading_deg",
        F.col("s.stop_sk").alias("nearest_stop_sk"),
        F.current_timestamp().alias("etl_loaded_at"),
    )

    fact_vp_filtered = fact_vp.filter(
        "vehicle_sk IS NOT NULL AND date_key IS NOT NULL AND time_key IS NOT NULL"
    )

    if fact_vp_filtered.count() > 0:
        (
            fact_vp_filtered.write
            .mode("append")
            .jdbc(config.POSTGRES_JDBC_URL, "dwh.fact_vehicle_position", properties=jdbc_props)
        )


def load_all_facts(spark, args, jdbc_props):
    load_fact_trip(spark, args, jdbc_props)
    load_fact_payment(spark, args, jdbc_props)
    load_fact_vehicle_position(spark, args, jdbc_props)

def main():
    args = parse_args()
    spark = get_spark()

    jdbc_props = {
        "user": config.POSTGRES_USER,
        "password": config.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    load_all_facts(spark, args, jdbc_props)

    spark.stop()

if __name__ == "__main__":
    main()
