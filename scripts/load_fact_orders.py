from pathlib import Path
import os

import psycopg2
from psycopg2.extras import execute_values

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_PATH = PROJECT_ROOT / "data" / "gold" / "fact_orders"

JDBC_URL = "jdbc:postgresql://localhost:5432/nordmarket"

JDBC_PROPERTIES = {
    "user": "dataforge",
    "password": os.getenv("DATAFORGE_DB_PASSWORD"),
    "driver": "org.postgresql.Driver",
}

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "nordmarket",
    "user": "dataforge",
    "password": os.getenv("DATAFORGE_DB_PASSWORD"),
}


spark = (
    SparkSession.builder
    .appName("dataforge-load-fact-orders")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)


# -------------------------------------------------------------------
# Read Gold fact_orders
# -------------------------------------------------------------------

fact_orders_gold = spark.read.parquet(str(GOLD_PATH))


# -------------------------------------------------------------------
# Read warehouse dimensions
# -------------------------------------------------------------------

dim_customer = (
    spark.read
    .jdbc(
        url=JDBC_URL,
        table="analytics.dim_customer",
        properties=JDBC_PROPERTIES,
    )
    .select(
        "customer_key",
        "customer_id",
    )
)


dim_date = (
    spark.read
    .jdbc(
        url=JDBC_URL,
        table="analytics.dim_date",
        properties=JDBC_PROPERTIES,
    )
    .select(
        "date_key",
        "full_date",
    )
)


# -------------------------------------------------------------------
# Build warehouse fact_orders dataset
# -------------------------------------------------------------------

fact_orders = (
    fact_orders_gold.alias("f")
    .join(
        dim_customer.alias("c"),
        F.col("f.customer_id") == F.col("c.customer_id"),
        "left",
    )
    .join(
        dim_date.alias("d"),
        F.to_date(F.col("f.order_purchase_timestamp"))
        == F.col("d.full_date"),
        "left",
    )
    .select(
        F.col("f.order_id").alias("order_id"),
        F.col("c.customer_key").alias("customer_key"),
        F.col("d.date_key").alias("order_date_key"),
        F.col("f.order_status").alias("order_status"),
        F.col("f.item_count").cast("int").alias("item_count"),
        F.col("f.product_count").cast("int").alias("product_count"),
        F.col("f.merchandise_value")
        .cast(DecimalType(18, 2))
        .alias("merchandise_value"),
        F.col("f.freight_value")
        .cast(DecimalType(18, 2))
        .alias("freight_value"),
        F.col("f.order_total_value")
        .cast(DecimalType(18, 2))
        .alias("order_total_value"),
        F.col("f.payment_count").cast("int").alias("payment_count"),
        F.col("f.payment_total_value")
        .cast(DecimalType(18, 2))
        .alias("payment_total_value"),
        F.col("f.delivery_days")
        .cast(DecimalType(10, 2))
        .alias("delivery_days"),
        F.col("f.estimated_delivery_days")
        .cast(DecimalType(10, 2))
        .alias("estimated_delivery_days"),
        F.col("f.delivery_delay_days")
        .cast(DecimalType(10, 2))
        .alias("delivery_delay_days"),
        F.col("f.order_purchase_timestamp")
        .alias("order_purchase_timestamp"),
        F.col("f.order_approved_at")
        .alias("order_approved_at"),
        F.col("f.order_delivered_carrier_date")
        .alias("order_delivered_carrier_date"),
        F.col("f.order_delivered_customer_date")
        .alias("order_delivered_customer_date"),
        F.col("f.order_estimated_delivery_date")
        .alias("order_estimated_delivery_date"),
    )
)


# -------------------------------------------------------------------
# Validation
# -------------------------------------------------------------------

gold_count = fact_orders_gold.count()
warehouse_count = fact_orders.count()

print(f"Gold fact_orders rows: {gold_count}")
print(f"Warehouse fact_orders rows to load: {warehouse_count}")


null_customer_key_count = (
    fact_orders
    .filter(
        F.col("customer_key").isNull()
        & F.col("order_id").isNotNull()
    )
    .count()
)

null_date_key_count = (
    fact_orders
    .filter(
        F.col("order_date_key").isNull()
        & F.col("order_purchase_timestamp").isNotNull()
    )
    .count()
)

print(f"Orders with missing customer_key: {null_customer_key_count}")
print(f"Orders with missing order_date_key: {null_date_key_count}")


duplicate_order_id_count = (
    fact_orders
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Duplicate order_id groups: {duplicate_order_id_count}")


if duplicate_order_id_count > 0:
    raise ValueError(
        "Duplicate order_id values detected in the fact_orders load dataset."
    )


# -------------------------------------------------------------------
# Show sample records
# -------------------------------------------------------------------

print("Sample fact_orders records:")

fact_orders.select(
    "order_id",
    "customer_key",
    "order_date_key",
    "order_status",
    "order_total_value",
).show(10, truncate=False)


# -------------------------------------------------------------------
# Convert Spark rows to PostgreSQL rows
# -------------------------------------------------------------------

rows = [
    (
        row.order_id,
        row.customer_key,
        row.order_date_key,
        row.order_status,
        row.item_count,
        row.product_count,
        row.merchandise_value,
        row.freight_value,
        row.order_total_value,
        row.payment_count,
        row.payment_total_value,
        row.delivery_days,
        row.estimated_delivery_days,
        row.delivery_delay_days,
        row.order_purchase_timestamp,
        row.order_approved_at,
        row.order_delivered_carrier_date,
        row.order_delivered_customer_date,
        row.order_estimated_delivery_date,
    )
    for row in fact_orders.collect()
]


# Spark is no longer needed after collecting the rows.
spark.stop()


# -------------------------------------------------------------------
# Idempotent PostgreSQL upsert
# -------------------------------------------------------------------

print("Writing fact_orders to PostgreSQL with idempotent upsert...")


conn = psycopg2.connect(**DB_CONFIG)

try:
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO analytics.fact_orders (
                order_id,
                customer_key,
                order_date_key,
                order_status,
                item_count,
                product_count,
                merchandise_value,
                freight_value,
                order_total_value,
                payment_count,
                payment_total_value,
                delivery_days,
                estimated_delivery_days,
                delivery_delay_days,
                order_purchase_timestamp,
                order_approved_at,
                order_delivered_carrier_date,
                order_delivered_customer_date,
                order_estimated_delivery_date
            )
            VALUES %s
            ON CONFLICT (order_id)
            DO UPDATE SET
                customer_key = EXCLUDED.customer_key,
                order_date_key = EXCLUDED.order_date_key,
                order_status = EXCLUDED.order_status,
                item_count = EXCLUDED.item_count,
                product_count = EXCLUDED.product_count,
                merchandise_value = EXCLUDED.merchandise_value,
                freight_value = EXCLUDED.freight_value,
                order_total_value = EXCLUDED.order_total_value,
                payment_count = EXCLUDED.payment_count,
                payment_total_value = EXCLUDED.payment_total_value,
                delivery_days = EXCLUDED.delivery_days,
                estimated_delivery_days = EXCLUDED.estimated_delivery_days,
                delivery_delay_days = EXCLUDED.delivery_delay_days,
                order_purchase_timestamp = EXCLUDED.order_purchase_timestamp,
                order_approved_at = EXCLUDED.order_approved_at,
                order_delivered_carrier_date = EXCLUDED.order_delivered_carrier_date,
                order_delivered_customer_date = EXCLUDED.order_delivered_customer_date,
                order_estimated_delivery_date = EXCLUDED.order_estimated_delivery_date
            """,
            rows,
            page_size=5000,
        )

    conn.commit()

finally:
    conn.close()


print("fact_orders idempotent load completed successfully.")