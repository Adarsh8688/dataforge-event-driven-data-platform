from pathlib import Path
import base64
from decimal import Decimal
import os
import psycopg2
from psycopg2.extras import execute_values

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "products"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "nordmarket",
    "user": "dataforge",
    "password": os.getenv("DATAFORGE_DB_PASSWORD"),
}


spark = (
    SparkSession.builder
    .appName("dataforge-load-dim-product")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)


def decode_decimal_struct(value, scale):
    if value is None:
        return None

    try:
        raw = base64.b64decode(value)
        integer_value = int.from_bytes(raw, byteorder="big", signed=True)
        return Decimal(integer_value) / (Decimal(10) ** Decimal(scale or 0))
    except Exception:
        return None


decode_decimal = F.udf(decode_decimal_struct, "decimal(10,2)")


products = spark.read.parquet(str(SILVER_PATH))

dim_product = (
    products
    .select(
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    )
    .dropDuplicates(["product_id"])
    .withColumn(
        "product_length_cm_decoded",
        decode_decimal(
            F.col("product_length_cm.value"),
            F.col("product_length_cm.scale"),
        ),
    )
    .withColumn(
        "product_height_cm_decoded",
        decode_decimal(
            F.col("product_height_cm.value"),
            F.col("product_height_cm.scale"),
        ),
    )
    .withColumn(
        "product_width_cm_decoded",
        decode_decimal(
            F.col("product_width_cm.value"),
            F.col("product_width_cm.scale"),
        ),
    )
    .select(
        "product_id",
        "product_category_name",
        F.col("product_name_lenght").alias("product_name_length"),
        F.col("product_description_lenght").alias("product_description_length"),
        "product_photos_qty",
        "product_weight_g",
        F.col("product_length_cm_decoded").alias("product_length_cm"),
        F.col("product_height_cm_decoded").alias("product_height_cm"),
        F.col("product_width_cm_decoded").alias("product_width_cm"),
    )
)


silver_count = products.count()
load_count = dim_product.count()

print(f"Silver product rows: {silver_count}")
print(f"Warehouse product rows to load: {load_count}")

print("Sample decoded product dimensions:")
dim_product.select(
    "product_id",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
).show(10, False)


rows = [
    (
        row.product_id,
        row.product_category_name,
        row.product_name_length,
        row.product_description_length,
        row.product_photos_qty,
        row.product_weight_g,
        row.product_length_cm,
        row.product_height_cm,
        row.product_width_cm,
    )
    for row in dim_product.collect()
]

spark.stop()

conn = psycopg2.connect(**DB_CONFIG)

try:
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO analytics.dim_product (
                product_id,
                product_category_name,
                product_name_length,
                product_description_length,
                product_photos_qty,
                product_weight_g,
                product_length_cm,
                product_height_cm,
                product_width_cm
            )
            VALUES %s
            ON CONFLICT (product_id)
            DO UPDATE SET
                product_category_name = EXCLUDED.product_category_name,
                product_name_length = EXCLUDED.product_name_length,
                product_description_length = EXCLUDED.product_description_length,
                product_photos_qty = EXCLUDED.product_photos_qty,
                product_weight_g = EXCLUDED.product_weight_g,
                product_length_cm = EXCLUDED.product_length_cm,
                product_height_cm = EXCLUDED.product_height_cm,
                product_width_cm = EXCLUDED.product_width_cm
            """,
            rows,
            page_size=5000,
        )

    conn.commit()

finally:
    conn.close()

print("dim_product idempotent load completed successfully.")
