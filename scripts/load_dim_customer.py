from pathlib import Path
import os
import psycopg2
from psycopg2.extras import execute_values

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "customers"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "nordmarket",
    "user": "dataforge",
    "password": os.getenv("DATAFORGE_DB_PASSWORD"),
}


spark = (
    SparkSession.builder
    .appName("dataforge-load-dim-customer")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

customers = spark.read.parquet(str(SILVER_PATH))

dim_customer = (
    customers
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "customer_zip_code_prefix",
    )
    .dropDuplicates(["customer_id"])
)

silver_count = customers.count()
load_count = dim_customer.count()

print(f"Silver customer rows: {silver_count}")
print(f"Warehouse customer rows to load: {load_count}")

rows = [
    (
        row.customer_id,
        row.customer_unique_id,
        row.customer_city,
        row.customer_state,
        row.customer_zip_code_prefix,
    )
    for row in dim_customer.collect()
]

spark.stop()

conn = psycopg2.connect(**DB_CONFIG)

try:
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO analytics.dim_customer (
                customer_id,
                customer_unique_id,
                customer_city,
                customer_state,
                customer_zip_code_prefix
            )
            VALUES %s
            ON CONFLICT (customer_id)
            DO UPDATE SET
                customer_unique_id = EXCLUDED.customer_unique_id,
                customer_city = EXCLUDED.customer_city,
                customer_state = EXCLUDED.customer_state,
                customer_zip_code_prefix = EXCLUDED.customer_zip_code_prefix
            """,
            rows,
            page_size=5000,
        )

    conn.commit()

finally:
    conn.close()

print("dim_customer idempotent load completed successfully.")
