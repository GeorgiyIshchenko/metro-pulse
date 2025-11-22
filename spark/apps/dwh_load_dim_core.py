from pyspark.sql import SparkSession, functions as F, Window
import argparse
import config


def get_spark():
    return (
        SparkSession.builder
        .appName("metropulse_dwh_load_dim_core")
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
    df = spark.read.jdbc(config.POSTGRES_JDBC_URL,
                         table_name, properties=jdbc_props)
    if args.batch_id:
        df = df.filter(F.col("etl_batch_id") == F.lit(args.batch_id))
    return df


def load_dim_user(spark, args, jdbc_props):
    stg = read_staging_table(spark, args, jdbc_props, "staging.stg_user")

    w = (
        Window.partitionBy("user_id")
        .orderBy(
            F.col("src_updated_at").desc_nulls_last(),
            F.col("src_created_at").desc_nulls_last(),
            F.col("etl_loaded_at").desc(),
        )
    )

    stg_latest = (
        stg
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    dim = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_user", properties=jdbc_props) \
        .select("user_id_nat")

    new_users = (
        stg_latest.alias("s")
        .join(dim.alias("d"), F.col("s.user_id") == F.col("d.user_id_nat"), "left_anti")
    )

    dim_ins = (
        new_users
        .select(
            F.col("user_id").alias("user_id_nat"),
            "email",
            "phone",
            "full_name",
            F.col("registration_ts").cast("date").alias("registration_dt"),
            "status",
            F.lit("oltp").alias("src_system"),
            F.current_timestamp().alias("etl_loaded_at"),
        )
    )

    if dim_ins.count() > 0:
        (
            dim_ins.write
            .mode("append")
            .jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_user", properties=jdbc_props)
        )


def load_dim_vehicle(spark, args, jdbc_props):
    stg = read_staging_table(spark, args, jdbc_props, "staging.stg_vehicle")

    w = (
        Window.partitionBy("vehicle_id")
        .orderBy(
            F.col("src_updated_at").desc_nulls_last(),
            F.col("src_created_at").desc_nulls_last(),
            F.col("etl_loaded_at").desc(),
        )
    )

    stg_latest = (
        stg
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    dim = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_vehicle", properties=jdbc_props) \
        .select("vehicle_id_nat")

    new_vehicles = (
        stg_latest.alias("s")
        .join(dim.alias("d"), F.col("s.vehicle_id") == F.col("d.vehicle_id_nat"), "left_anti")
    )

    dim_ins = (
        new_vehicles
        .select(
            F.col("vehicle_id").alias("vehicle_id_nat"),
            "registration_number",
            "vehicle_type",
            "capacity",
            "operator_name",
            F.lit("oltp_or_stream").alias("src_system"),
            F.current_timestamp().alias("etl_loaded_at"),
        )
    )

    if dim_ins.count() > 0:
        (
            dim_ins.write
            .mode("append")
            .jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_vehicle", properties=jdbc_props)
        )


def load_dim_stop(spark, args, jdbc_props):
    stg = read_staging_table(spark, args, jdbc_props, "staging.stg_stop")

    w = (
        Window.partitionBy("stop_id")
        .orderBy(
            F.col("src_updated_at").desc_nulls_last(),
            F.col("src_created_at").desc_nulls_last(),
            F.col("etl_loaded_at").desc(),
        )
    )

    stg_latest = (
        stg
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    dim = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_stop", properties=jdbc_props) \
        .select("stop_id_nat")

    new_stops = (
        stg_latest.alias("s")
        .join(dim.alias("d"), F.col("s.stop_id") == F.col("d.stop_id_nat"), "left_anti")
    )

    dim_ins = (
        new_stops
        .select(
            F.col("stop_id").alias("stop_id_nat"),
            "stop_code",
            "stop_name",
            "city",
            "latitude",
            "longitude",
            "zone",
            F.lit("oltp").alias("src_system"),
            F.current_timestamp().alias("etl_loaded_at"),
        )
    )

    if dim_ins.count() > 0:
        (
            dim_ins.write
            .mode("append")
            .jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_stop", properties=jdbc_props)
        )


def load_dim_fare_product(spark, args, jdbc_props):
    stg = read_staging_table(spark, args, jdbc_props,
                             "staging.stg_fare_product")

    w = (
        Window.partitionBy("fare_product_id")
        .orderBy(
            F.col("src_updated_at").desc_nulls_last(),
            F.col("src_created_at").desc_nulls_last(),
            F.col("etl_loaded_at").desc(),
        )
    )

    stg_latest = (
        stg
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    dim = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_fare_product", properties=jdbc_props) \
        .select("fare_product_id_nat")

    new_fp = (
        stg_latest.alias("s")
        .join(dim.alias("d"), F.col("s.fare_product_id") == F.col("d.fare_product_id_nat"), "left_anti")
    )

    dim_ins = (
        new_fp
        .select(
            F.col("fare_product_id").alias("fare_product_id_nat"),
            "product_name",
            "fare_type",
            F.coalesce(F.col("currency_code"), F.lit(
                "EUR")).alias("currency_code"),
            "base_price",
            "description",
            F.lit("oltp").alias("src_system"),
            F.current_timestamp().alias("etl_loaded_at"),
        )
    )

    if dim_ins.count() > 0:
        (
            dim_ins.write
            .mode("append")
            .jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_fare_product", properties=jdbc_props)
        )


def load_all_dims(spark, args, jdbc_props):
    load_dim_user(spark, args, jdbc_props)
    load_dim_vehicle(spark, args, jdbc_props)
    load_dim_stop(spark, args, jdbc_props)
    load_dim_fare_product(spark, args, jdbc_props)


def main():
    args = parse_args()
    spark = get_spark()

    jdbc_props = {
        "user": config.POSTGRES_USER,
        "password": config.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    load_all_dims(spark, args, jdbc_props)

    spark.stop()


if __name__ == "__main__":
    main()
