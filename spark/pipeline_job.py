from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, DataFrame

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
REF_FILE = ROOT / "data" / "reference" / "taxi_zone_lookup.csv"
OUTPUT_DIR = ROOT / "data" / "spark"


def _require_inputs() -> None:
    if not RAW_DIR.exists() or not list(RAW_DIR.glob("yellow_tripdata_*.parquet")):
        raise FileNotFoundError("Missing raw parquet files in data/raw. Run the download step first.")
    if not REF_FILE.exists():
        raise FileNotFoundError("Missing taxi_zone_lookup.csv in data/reference. Run the download step first.")


def _write_df(df: DataFrame, relative_path: str) -> None:
    target = OUTPUT_DIR / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(target))


def run_spark_pipeline() -> None:
    _require_inputs()

    spark = (
        SparkSession.builder.appName("nyc-taxi-medallion-spark")
        .config("spark.sql.adaptive.enabled", True)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_path = str(RAW_DIR / "yellow_tripdata_*.parquet")
    print(f"Reading bronze data from {raw_path}")
    df_raw = spark.read.parquet(raw_path)
    _write_df(df_raw, "bronze/yellow_tripdata")

    df_lookup = (
        spark.read.option("header", True).csv(str(REF_FILE))
        .select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.trim("Borough").alias("borough"),
            F.trim("Zone").alias("zone"),
            F.trim("service_zone").alias("service_zone"),
        )
    )

    base_cols = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
    ]

    df_base = (
        df_raw.select(*base_cols)
        .withColumn("trip_minutes", (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) / 60.0)
        .withColumn(
            "cbd_congestion_fee",
            F.coalesce(F.col("cbd_congestion_fee"), F.lit(0.0)),
        )
        .withColumn(
            "gross_revenue",
            F.col("fare_amount")
            + F.col("tip_amount")
            + F.col("tolls_amount")
            + F.col("congestion_surcharge")
            + F.col("cbd_congestion_fee"),
        )
        .withColumn(
            "fare_per_mile",
            F.when(F.col("trip_distance") > 0, F.col("fare_amount") / F.col("trip_distance")),
        )
    )

    invalid_expr = (
        (F.col("trip_distance") <= 0)
        | (F.col("fare_amount") <= 0)
        | (F.col("passenger_count") < 1)
        | (F.col("passenger_count") > 6)
        | (F.col("trip_minutes") < 1)
        | (F.col("trip_minutes") > 240)
        | F.col("PULocationID").isNull()
        | F.col("DOLocationID").isNull()
    )

    df_clean = (
        df_base.withColumn("invalid_trip_flag", invalid_expr)
        .filter(~invalid_expr)
        .cache()
    )

    _write_df(df_clean, "silver/yellow_tripdata_clean")

    pickup_ts = F.col("tpep_pickup_datetime")
    df_zone_metrics = (
        df_clean.join(df_lookup, df_clean.PULocationID == df_lookup.location_id, "inner")
        .withColumn("pickup_hour", F.date_trunc("hour", pickup_ts))
        .groupBy("pickup_hour", "borough", "zone")
        .agg(
            F.count("*").alias("trips"),
            F.sum("passenger_count").alias("passengers"),
            F.sum("trip_distance").alias("total_miles"),
            F.avg("trip_distance").alias("avg_miles"),
            F.sum("gross_revenue").alias("gross_revenue"),
            F.sum("cbd_congestion_fee").alias("congestion_fees"),
            F.sum("tip_amount").alias("tips"),
            F.sum(F.when(F.col("RatecodeID") == 2, 1).otherwise(0)).alias("jfk_trips"),
        )
    )
    _write_df(df_zone_metrics, "gold/zone_hourly_metrics")

    payment_dim = spark.createDataFrame(
        [
            (1, "Credit Card"),
            (2, "Cash"),
            (3, "No Charge"),
            (4, "Dispute"),
            (5, "Unknown"),
            (6, "Voided"),
        ],
        ["payment_type", "description"],
    )

    df_payment_metrics = (
        df_clean.withColumn("service_day", F.date_trunc("day", pickup_ts))
        .join(payment_dim, "payment_type", "left")
        .groupBy("service_day", "description")
        .agg(
            F.count("*").alias("trips"),
            F.sum("gross_revenue").alias("revenue"),
            F.avg("fare_per_mile").alias("fare_per_mile"),
        )
    )
    _write_df(df_payment_metrics, "gold/payment_type_metrics")

    bronze_rows = df_raw.count()
    invalid_rows = df_raw.filter(invalid_expr).count()
    silver_rows = df_clean.count()

    df_quality = spark.createDataFrame(
        [(datetime.now(timezone.utc), bronze_rows, invalid_rows, silver_rows)],
        ["generated_at", "bronze_rows", "invalid_bronze_rows", "silver_rows"],
    )
    _write_df(df_quality, "silver/data_quality_summary")

    spark.stop()
    print("Spark pipeline finished. Outputs written under data/spark.")


if __name__ == "__main__":
    run_spark_pipeline()
