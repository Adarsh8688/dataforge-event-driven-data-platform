from pathlib import Path
import os
import psycopg2
from psycopg2.extras import execute_values

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "orders"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "nordmarket",
    "user": "dataforge",
    "password": os.getenv("DATAFORGE_DB_PASSWORD"),
}


spark = (
    SparkSession.builder
    .appName("dataforge-load-dim-date")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)


orders = spark.read.parquet(str(SILVER_PATH))

dates = (
    orders
    .select(
        F.to_date(
            F.timestamp_micros(
                F.col("order_purchase_timestamp")
            )
        ).alias("full_date")
    )
    .where(F.col("full_date").isNotNull())
    .agg(
        F.min("full_date").alias("min_date"),
        F.max("full_date").alias("max_date"),
    )
    .collect()[0]
)

min_date = dates["min_date"]
max_date = dates["max_date"]

print(f"Minimum order date: {min_date}")
print(f"Maximum order date: {max_date}")

calendar = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.lit(min_date),
                F.lit(max_date),
                F.expr("INTERVAL 1 DAY"),
            )
        ).alias("full_date")
    )
)

dim_date = (
    calendar
    .select(
        F.date_format("full_date", "yyyyMMdd").cast("integer").alias("date_key"),
        "full_date",
        F.dayofmonth("full_date").alias("day_of_month"),
        F.dayofweek("full_date").alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.weekofyear("full_date").alias("week_of_year"),
        F.month("full_date").alias("month_number"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.quarter("full_date").alias("quarter_number"),
        F.year("full_date").alias("year_number"),
    )
    .dropDuplicates(["full_date"])
    .orderBy("full_date")
)

load_count = dim_date.count()

print(f"Warehouse date rows to load: {load_count}")

rows = [
    (
        row.date_key,
        row.full_date,
        row.day_of_month,
        row.day_of_week,
        row.day_name,
        row.week_of_year,
        row.month_number,
        row.month_name,
        row.quarter_number,
        row.year_number,
    )
    for row in dim_date.collect()
]

spark.stop()

conn = psycopg2.connect(**DB_CONFIG)

try:
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO analytics.dim_date (
                date_key,
                full_date,
                day_of_month,
                day_of_week,
                day_name,
                week_of_year,
                month_number,
                month_name,
                quarter_number,
                year_number
            )
            VALUES %s
            ON CONFLICT (date_key)
            DO UPDATE SET
                full_date = EXCLUDED.full_date,
                day_of_month = EXCLUDED.day_of_month,
                day_of_week = EXCLUDED.day_of_week,
                day_name = EXCLUDED.day_name,
                week_of_year = EXCLUDED.week_of_year,
                month_number = EXCLUDED.month_number,
                month_name = EXCLUDED.month_name,
                quarter_number = EXCLUDED.quarter_number,
                year_number = EXCLUDED.year_number
            """,
            rows,
            page_size=1000,
        )

    conn.commit()

finally:
    conn.close()

print("dim_date idempotent load completed successfully.")
