from pathlib import Path
import base64
from decimal import Decimal

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "payments"
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "payments"


spark = (
    SparkSession.builder
    .appName("dataforge-payments-silver")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)


def decode_money_value(value):
    """
    Decode Debezium PostgreSQL NUMERIC values.

    Debezium currently serializes PostgreSQL NUMERIC values as
    Base64-encoded signed big-endian integers.

    Olist monetary fields use scale 2:
        integer_value / 100 = monetary value
    """
    if value is None:
        return None

    try:
        raw = base64.b64decode(value)

        integer_value = int.from_bytes(
            raw,
            byteorder="big",
            signed=True,
        )

        return Decimal(integer_value) / Decimal("100")

    except Exception:
        return None


decode_money = F.udf(
    decode_money_value,
    "decimal(18,2)",
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
    .filter(
        F.col("order_id").isNotNull()
        & F.col("payment_sequential").isNotNull()
    )
)


latest_record_window = Window.partitionBy(
    "order_id",
    "payment_sequential",
).orderBy(
    F.col("cdc_timestamp_ms").desc(),
    F.col("cdc_lsn").desc(),
    F.col("kafka_offset").desc(),
)


payments_silver = (
    active_records
    .withColumn(
        "row_number",
        F.row_number().over(latest_record_window),
    )
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)


# Normalize Debezium PostgreSQL NUMERIC payment values.
payments_silver = (
    payments_silver
    .withColumn(
        "payment_value",
        decode_money(F.col("payment_value")),
    )
)


payments_silver.write.mode("overwrite").parquet(
    str(SILVER_PATH)
)


print(f"Bronze event count: {bronze.count()}")
print(f"Silver payment count: {payments_silver.count()}")


duplicate_count = (
    payments_silver
    .groupBy("order_id", "payment_sequential")
    .count()
    .filter(F.col("count") > 1)
    .count()
)


print(f"Duplicate payment keys: {duplicate_count}")


spark.stop()