import psycopg2


DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "nordmarket",
    "user": "dataforge",
    "password": os.getenv("DATAFORGE_DB_PASSWORD"),
}


CHECKS = [
    (
        "analytics.fact_orders.customer_key",
        """
        SELECT COUNT(*)
        FROM analytics.fact_orders f
        LEFT JOIN analytics.dim_customer d
            ON f.customer_key = d.customer_key
        WHERE f.customer_key IS NOT NULL
          AND d.customer_key IS NULL;
        """,
    ),
    (
        "analytics.fact_orders.order_date_key",
        """
        SELECT COUNT(*)
        FROM analytics.fact_orders f
        LEFT JOIN analytics.dim_date d
            ON f.order_date_key = d.date_key
        WHERE f.order_date_key IS NOT NULL
          AND d.date_key IS NULL;
        """,
    ),
    (
        "analytics.fact_order_items.product_key",
        """
        SELECT COUNT(*)
        FROM analytics.fact_order_items f
        LEFT JOIN analytics.dim_product d
            ON f.product_key = d.product_key
        WHERE f.product_key IS NOT NULL
          AND d.product_key IS NULL;
        """,
    ),
    (
        "analytics.fact_order_items.order_date_key",
        """
        SELECT COUNT(*)
        FROM analytics.fact_order_items f
        LEFT JOIN analytics.dim_date d
            ON f.order_date_key = d.date_key
        WHERE f.order_date_key IS NOT NULL
          AND d.date_key IS NULL;
        """,
    ),
]


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    failures = []

    try:
        with conn.cursor() as cur:
            for check_name, query in CHECKS:
                cur.execute(query)
                orphan_count = cur.fetchone()[0]

                print(
                    f"{check_name}: "
                    f"orphan_count={orphan_count}"
                )

                if orphan_count != 0:
                    failures.append(
                        f"{check_name}: "
                        f"found {orphan_count} orphaned foreign keys"
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