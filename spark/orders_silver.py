from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "orders"
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "orders"

spark = (
    SparkSession.builder
    .appName("dataforge-orders-silver")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

bronze = spark.read.json(str(BRONZE_PATH))

events = bronze.select(
    F.col("payload.payload.after").alias("after"),
    F.col("payload.payload.op").alias("cdc_operation"),
    F.coalesce(
        F.col("payload.payload.ts_ms"),
        F.col("payload.payload.source.ts_ms"),
        F.lit(0),
    ).alias("cdc_timestamp_ms"),
    F.coalesce(
        F.col("payload.payload.source.lsn"),
        F.lit(-1),
    ).alias("cdc_lsn"),
    F.col("kafka_partition"),
    F.col("kafka_offset"),
)

active_records = (
    events
    .filter(F.col("after").isNotNull())
    .select(
        "after.*",
        "cdc_operation",
        "cdc_timestamp_ms",
        "cdc_lsn",
        "kafka_partition",
        "kafka_offset",
    )
    .filter(F.col("order_id").isNotNull())
)

latest_record_window = Window.partitionBy("order_id").orderBy(
    F.col("cdc_timestamp_ms").desc(),
    F.col("cdc_lsn").desc(),
    F.col("kafka_offset").desc(),
)

orders_silver = (
    active_records
    .withColumn("row_number", F.row_number().over(latest_record_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

orders_silver.write.mode("overwrite").parquet(str(SILVER_PATH))

print(f"Bronze event count: {bronze.count()}")
print(f"Silver order count: {orders_silver.count()}")

duplicate_count = (
    orders_silver.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Duplicate order IDs: {duplicate_count}")

spark.stop()