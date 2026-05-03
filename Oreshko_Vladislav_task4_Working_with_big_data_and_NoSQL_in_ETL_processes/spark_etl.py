"""
Task 4 - Webinar 11: Working with Big Data in ETL processes
Author: Vladislav Oreshko
Course: ETL-processes (HSE / Netology)

Pipeline:
  S3 (sensors.json) → Spark (explode readings + alerts) → S3 (Parquet)
"""

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# ---------------------------------------------------------------------------
# Spark session – uses Yandex Data Processing S3A connector
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("IoT_Sensors_ETL")
    .getOrCreate()
)

S3_BUCKET = "s3a://iot-sensors-vlad"
INPUT_PATH  = f"{S3_BUCKET}/sensors.json"
OUT_READINGS = f"{S3_BUCKET}/readings.parquet"
OUT_ALERTS   = f"{S3_BUCKET}/alerts.parquet"

# ---------------------------------------------------------------------------
# 1. READ
# ---------------------------------------------------------------------------
df = (
    spark.read
    .option("multiline", "true")
    .json(INPUT_PATH)
)

print("=== Raw schema ===")
df.printSchema()
print(f"Stations loaded: {df.count()}")
df.show(truncate=False)

# ---------------------------------------------------------------------------
# 2. TRANSFORM – explode nested arrays
# ---------------------------------------------------------------------------

# ── readings (one row per sensor measurement) ────────────────────────────
exploded_readings = df.select(
    "station_id",
    "location",
    "datetime",
    F.explode("readings").alias("reading")
)

readings = exploded_readings.select(
    "station_id",
    "location",
    "datetime",
    F.col("reading.sensor_type").alias("sensor_type"),
    F.col("reading.value").alias("value"),
    F.col("reading.unit").alias("unit"),
    F.col("reading.status").alias("status"),
)

print("=== Readings (flattened) ===")
readings.show(truncate=False)

# ── alerts (one row per alert message) ──────────────────────────────────
exploded_alerts = df.select(
    "station_id",
    "location",
    "datetime",
    F.explode("alerts").alias("alert")
)

alerts = exploded_alerts.select(
    "station_id",
    "location",
    "datetime",
    F.col("alert.level").alias("alert_level"),
    F.col("alert.code").alias("alert_code"),
    F.col("alert.message").alias("alert_message"),
)

print("=== Alerts (flattened) ===")
alerts.show(truncate=False)

# ---------------------------------------------------------------------------
# 3. WRITE – Parquet back to S3
# ---------------------------------------------------------------------------
readings.write.mode("overwrite").parquet(OUT_READINGS)
print(f"Readings written → {OUT_READINGS}")

alerts.write.mode("overwrite").parquet(OUT_ALERTS)
print(f"Alerts written   → {OUT_ALERTS}")

spark.stop()
