import psycopg2


DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "nordmarket",
    "user": "dataforge",
    "password": os.getenv("DATAFORGE_DB_PASSWORD"),
}


EXPECTED_COUNTS = {
    "analytics.dim_customer": 99441,
    "analytics.dim_product": 32951,
    "analytics.dim_date": 774,
    "analytics.fact_orders": 99441,
    "analytics.fact_order_items": 112650,
}


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    failures = []

    try:
        with conn.cursor() as cur:
            for table_name, expected_count in EXPECTED_COUNTS.items():
                cur.execute(f"SELECT COUNT(*) FROM {table_name};")
                actual_count = cur.fetchone()[0]

                print(
                    f"{table_name}: "
                    f"expected={expected_count}, "
                    f"actual={actual_count}"
                )

                if actual_count != expected_count:
                    failures.append(
                        f"{table_name}: "
                        f"expected {expected_count}, "
                        f"found {actual_count}"
                    )

    finally:
        conn.close()

    if failures:
        print("\nDATA QUALITY CHECK FAILED")

        for failure in failures:
            print(f"- {failure}")

        raise SystemExit(1)

    print("\nDATA QUALITY CHECK PASSED")


if __name__ == "__main__":
    main()