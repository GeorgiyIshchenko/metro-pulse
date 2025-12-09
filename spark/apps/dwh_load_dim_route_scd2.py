from pyspark.sql import SparkSession, functions as F, Window
import argparse
import config


def get_spark():
    return (
        SparkSession.builder
        .appName("metropulse_dim_route_scd2")
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


def main():
    args = parse_args()
    spark = get_spark()

    jdbc_props = {
        "user": config.POSTGRES_USER,
        "password": config.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    # 1. staging маршруты только текущего батча
    stg = spark.read.jdbc(config.POSTGRES_JDBC_URL, "staging.stg_route", properties=jdbc_props)
    if args.batch_id:
        stg = stg.filter(F.col("etl_batch_id") == F.lit(args.batch_id))

    if stg.rdd.isEmpty():
        print(f"No routes found in staging.stg_route for batch_id={args.batch_id}")
        spark.stop()
        return

    dim_fare = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_fare_product", properties=jdbc_props) \
        .select("fare_product_sk", "fare_product_id_nat")

    stg = (
        stg.alias("s")
        .join(
            dim_fare.alias("f"),
            F.col("s.default_fare_product_id") == F.col("f.fare_product_id_nat"),
            "left",
        )
        .withColumnRenamed("fare_product_sk", "default_fare_product_sk")
    )

    stg = (
        stg
        .withColumn(
            "valid_from",
            F.coalesce(
                F.to_date("valid_from_ts"),
                F.to_date("src_created_at"),
                F.current_date(),
            ),
        )
        .withColumn(
            "valid_to_candidate",
            F.coalesce(
                F.to_date("valid_to_ts"),
                F.to_date(F.lit("9999-12-31")),
            ),
        )
    )

    w = Window.partitionBy("route_id").orderBy(
        F.col("src_updated_at").desc_nulls_last(),
        F.col("etl_loaded_at").desc(),
    )

    stg_latest = (
        stg
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    dim = spark.read.jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_route", properties=jdbc_props)

    dim_current = dim.filter("is_current = TRUE")

    joined = (
        stg_latest.alias("s")
        .join(
            dim_current.alias("d"),
            F.col("s.route_id") == F.col("d.route_id_nat"),
            "left",
        )
    )

    is_new = F.col("d.route_sk").isNull()

    is_changed = (
        (~is_new) & (
            (F.col("s.route_code")            != F.col("d.route_code")) |
            (F.col("s.route_name")            != F.col("d.route_name")) |
            (F.col("s.route_type")            != F.col("d.route_type")) |
            (F.col("s.description")           != F.col("d.description")) |
            (F.col("s.route_pattern_hash")    != F.col("d.route_pattern_hash")) |
            (F.col("s.default_fare_product_sk") != F.col("d.default_fare_product_sk")) |
            (F.col("s.valid_from")            >  F.col("d.valid_from"))  # новая версия позже
        )
    )

    to_insert = joined.filter(is_new | is_changed)

    if to_insert.rdd.isEmpty():
        print(f"No new or changed routes for batch_id={args.batch_id}")
        spark.stop()
        return

    new_versions = to_insert.select(
        F.col("s.route_id").alias("route_id_nat"),
        F.col("s.route_code").alias("route_code"),
        F.col("s.route_name").alias("route_name"),
        F.col("s.route_type").alias("route_type"),
        F.col("s.description").alias("description"),
        F.col("s.route_pattern_hash").alias("route_pattern_hash"),
        F.col("s.default_fare_product_sk").alias("default_fare_product_sk"),
        F.col("s.valid_from").alias("valid_from"),
        # valid_to всегда "открытый конец", пока триггер не закроет старую запись
        F.to_date(F.lit("9999-12-31")).alias("valid_to"),
        F.lit(True).alias("is_current"),
        F.col("s.src_system").alias("src_system"),
        F.current_timestamp().alias("etl_loaded_at"),
    )

    # Drop duplicates already present in dim_route for same (route_id_nat, valid_from)
    dim_pk = dim.select("route_id_nat", "valid_from")
    new_versions = new_versions.join(
        dim_pk,
        (new_versions.route_id_nat == dim_pk.route_id_nat) &
        (new_versions.valid_from == dim_pk.valid_from),
        how="left_anti",
    )

    (
        new_versions.write
        .mode("append")
        .jdbc(config.POSTGRES_JDBC_URL, "dwh.dim_route", properties=jdbc_props)
    )

    print(f"Inserted {new_versions.count()} new/changed dim_route rows for batch_id={args.batch_id}")

    spark.stop()


if __name__ == "__main__":
    main()
