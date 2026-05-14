import json
import os
import sys
from datetime import datetime

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, lit, lower, trim, when
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Schema for the cleaned DataFrame ──────────────────────────────────────────
CLEAN_SCHEMA = StructType([
    StructField("city",        StringType(),    True),
    StructField("temp_c",      FloatType(),     True),
    StructField("feels_like",  FloatType(),     True),
    StructField("humidity",    IntegerType(),   True),
    StructField("pressure",    IntegerType(),   True),
    StructField("wind_speed",  FloatType(),     True),
    StructField("clouds",      IntegerType(),   True),
    StructField("description", StringType(),    True),
    StructField("recorded_at", TimestampType(), True),
])


def flatten_raw_json(raw: dict) -> Row:
    ts = datetime.utcfromtimestamp(raw.get("dt", 0))

    return Row(
        city        = str(raw.get("name", "unknown")),
        temp_c      = float(raw["main"]["temp"]),
        feels_like  = float(raw["main"].get("feels_like", 0)),
        humidity    = int(raw["main"].get("humidity", 0)),
        pressure    = int(raw["main"].get("pressure", 0)),
        wind_speed  = float(raw.get("wind", {}).get("speed", 0.0)),
        clouds      = int(raw.get("clouds", {}).get("all", 0)),
        description = str(raw["weather"][0]["description"])
                      if raw.get("weather") else "unknown",
        recorded_at = ts,
    )


def transform(raw_path: str, output_path: str):
    """Main transform function."""

    # ── 1. Start Spark ────────────────────────────────────────────────────────
    spark = SparkSession.builder \
        .appName("WeatherETL_Transform") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(f"\n{'='*60}")
    print("  WeatherETL — Transform Step")
    print(f"  Input  : {raw_path}")
    print(f"  Output : {output_path}")
    print(f"{'='*60}\n")

    # ── 2. Read raw JSON ──────────────────────────────────────────────────────
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw JSON not found: {raw_path}")

    with open(raw_path, "r") as f:
        raw = json.load(f)

    # Support both single record (dict) and multiple records (list)
    if isinstance(raw, dict):
        rows = [flatten_raw_json(raw)]
    elif isinstance(raw, list):
        rows = [flatten_raw_json(r) for r in raw]
    else:
        raise ValueError("Raw JSON must be a dict or list of dicts")

    print(f"  Records read from JSON: {len(rows)}")

    df = spark.createDataFrame(rows, schema=CLEAN_SCHEMA)

    # ── 3. Clean & transform ─────────────────────────────────────────────────
    df_clean = df \
        .withColumn("city",
            lower(trim(col("city")))) \
        .withColumn("description",
            lower(trim(col("description")))) \
        .withColumn("temp_c",
            spark_round(col("temp_c"), 1)) \
        .withColumn("feels_like",
            spark_round(col("feels_like"), 1)) \
        .withColumn("wind_speed",
            spark_round(col("wind_speed"), 2)) \
        .withColumn("heat_index",
            when(col("temp_c") >= 38, lit("extreme"))
            .when(col("temp_c") >= 35, lit("very hot"))
            .when(col("temp_c") >= 28, lit("hot"))
            .when(col("temp_c") >= 20, lit("warm"))
            .when(col("temp_c") >= 10, lit("cool"))
            .otherwise(lit("cold"))) \
        .filter(col("temp_c").isNotNull()) \
        .filter(col("humidity").between(0, 100)) \
        .filter(col("temp_c").between(-60, 60))

    # ── 4. Show summary ───────────────────────────────────────────────────────
    print("\n  Cleaned DataFrame preview:")
    df_clean.show(truncate=False)
    print("\n  Schema:")
    df_clean.printSchema()
    print(f"\n  Total clean rows: {df_clean.count()}")

    # ── 5. Write Parquet ──────────────────────────────────────────────────────
    os.makedirs(output_path, exist_ok=True)

    df_clean.write \
        .mode("overwrite") \
        .parquet(output_path)

    print(f"\n  Written to {output_path}")
    print(f"{'='*60}\n")

    spark.stop()


# ── Entrypoint ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit transform.py <raw_json_path> <output_parquet_path>")
        sys.exit(1)

    raw_json_path   = sys.argv[1]
    output_parq_path = sys.argv[2]

    transform(raw_json_path, output_parq_path)
