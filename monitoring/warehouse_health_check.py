import psycopg2
import os


DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "nordmarket",
    "user": "dataforge",
    "password": os.getenv("DATAFORGE_DB_PASSWORD")
}


TABLES = [
    "analytics.dim_customer",
    "analytics.dim_product",
    "analytics.dim_date",
    "analytics.fact_orders",
    "analytics.fact_order_items",
]


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    failures = []

    try:
        with conn.cursor() as cur:
            for table_name in TABLES:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table_name};")
                    row_count = cur.fetchone()[0]

                    print(
                        f"{table_name}: UP "
                        f"(rows={row_count})"
                    )

                except Exception as exc:
                    print(
                        f"{table_name}: DOWN - {exc}"
                    )
                    failures.append(table_name)

    finally:
        conn.close()

    print()

    if failures:
        print("WAREHOUSE HEALTH CHECK FAILED")

        for table_name in failures:
            print(f"- {table_name}")

        raise SystemExit(1)

    print("WAREHOUSE HEALTH CHECK PASSED")


if __name__ == "__main__":
    main()