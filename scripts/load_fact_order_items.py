from pathlib import Path
import os
import psycopg2
from psycopg2.extras import execute_values

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "order_items"

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


# -------------------------------------------------------------------
# Start Spark
# -------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("dataforge-load-fact-order-items")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)


# -------------------------------------------------------------------
# Read Silver order_items
# -------------------------------------------------------------------

order_items_silver = spark.read.parquet(str(SILVER_PATH))


# -------------------------------------------------------------------
# Read warehouse dimensions
# -------------------------------------------------------------------

dim_product = (
    spark.read
    .jdbc(
        url=JDBC_URL,
        table="analytics.dim_product",
        properties=JDBC_PROPERTIES,
    )
    .select(
        "product_key",
        "product_id",
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
# Build fact_order_items
# -------------------------------------------------------------------

fact_order_items = (
    order_items_silver.alias("oi")
    .join(
        dim_product.alias("p"),
        F.col("oi.product_id") == F.col("p.product_id"),
        "left",
    )
    .join(
        dim_date.alias("d"),
        F.to_date(
            F.timestamp_micros(
                F.col("oi.shipping_limit_date")
            )
        ) == F.col("d.full_date"),
        "left",
    )
    .select(
        F.col("oi.order_id").alias("order_id"),
        F.col("oi.order_item_id")
        .cast("int")
        .alias("order_item_id"),
        F.col("p.product_key").alias("product_key"),
        F.col("d.date_key").alias("order_date_key"),
        F.col("oi.seller_id").alias("seller_id"),
        F.timestamp_micros(
            F.col("oi.shipping_limit_date")
        ).alias("shipping_limit_date"),
        F.col("oi.price")
        .cast(DecimalType(18, 2))
        .alias("price"),
        F.col("oi.freight_value")
        .cast(DecimalType(18, 2))
        .alias("freight_value"),
    )
)


# -------------------------------------------------------------------
# Validation
# -------------------------------------------------------------------

silver_count = order_items_silver.count()
warehouse_count = fact_order_items.count()

print(f"Silver order_items rows: {silver_count}")
print(f"Warehouse fact_order_items rows to load: {warehouse_count}")


duplicate_key_count = (
    fact_order_items
    .groupBy("order_id", "order_item_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Duplicate order item keys: {duplicate_key_count}")


if duplicate_key_count > 0:
    raise ValueError(
        "Duplicate (order_id, order_item_id) keys detected "
        "in the fact_order_items load dataset."
    )


missing_product_key_count = (
    fact_order_items
    .filter(F.col("product_key").isNull())
    .count()
)

missing_date_key_count = (
    fact_order_items
    .filter(
        F.col("order_date_key").isNull()
        & F.col("shipping_limit_date").isNotNull()
    )
    .count()
)

print(
    f"Order items with missing product_key: "
    f"{missing_product_key_count}"
)

print(
    f"Order items with missing order_date_key: "
    f"{missing_date_key_count}"
)


# -------------------------------------------------------------------
# Show sample records
# -------------------------------------------------------------------

print("Sample fact_order_items records:")

fact_order_items.select(
    "order_id",
    "order_item_id",
    "product_key",
    "order_date_key",
    "seller_id",
    "price",
    "freight_value",
).show(10, truncate=False)


# -------------------------------------------------------------------
# Convert Spark rows to PostgreSQL rows
# -------------------------------------------------------------------

rows = [
    (
        row.order_id,
        row.order_item_id,
        row.product_key,
        row.order_date_key,
        row.seller_id,
        row.shipping_limit_date,
        row.price,
        row.freight_value,
    )
    for row in fact_order_items.collect()
]


# Spark is no longer needed after collecting the rows.
spark.stop()


# -------------------------------------------------------------------
# Idempotent PostgreSQL upsert
# -------------------------------------------------------------------

print(
    "Writing fact_order_items to PostgreSQL "
    "with idempotent upsert..."
)


conn = psycopg2.connect(**DB_CONFIG)

try:
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO analytics.fact_order_items (
                order_id,
                order_item_id,
                product_key,
                order_date_key,
                seller_id,
                shipping_limit_date,
                price,
                freight_value
            )
            VALUES %s
            ON CONFLICT (order_id, order_item_id)
            DO UPDATE SET
                product_key = EXCLUDED.product_key,
                order_date_key = EXCLUDED.order_date_key,
                seller_id = EXCLUDED.seller_id,
                shipping_limit_date = EXCLUDED.shipping_limit_date,
                price = EXCLUDED.price,
                freight_value = EXCLUDED.freight_value
            """,
            rows,
            page_size=5000,
        )

    conn.commit()

finally:
    conn.close()


print("fact_order_items idempotent load completed successfully.")