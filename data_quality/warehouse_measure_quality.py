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
        "analytics.fact_orders.item_count",
        """
        SELECT COUNT(*)
        FROM analytics.fact_orders
        WHERE item_count IS NULL
           OR item_count < 0;
        """,
    ),
    (
        "analytics.fact_orders.product_count",
        """
        SELECT COUNT(*)
        FROM analytics.fact_orders
        WHERE product_count IS NULL
           OR product_count < 0;
        """,
    ),
    (
        "analytics.fact_orders.merchandise_value",
        """
        SELECT COUNT(*)
        FROM analytics.fact_orders
        WHERE merchandise_value IS NULL
           OR merchandise_value < 0;
        """,
    ),
    (
        "analytics.fact_orders.freight_value",
        """
        SELECT COUNT(*)
        FROM analytics.fact_orders
        WHERE freight_value IS NULL
           OR freight_value < 0;
        """,
    ),
    (
        "analytics.fact_orders.order_total_value",
        """
        SELECT COUNT(*)
        FROM analytics.fact_orders
        WHERE order_total_value IS NULL
           OR order_total_value < 0;
        """,
    ),
    (
        "analytics.fact_orders.payment_count",
        """
        SELECT COUNT(*)
        FROM analytics.fact_orders
        WHERE payment_count IS NULL
           OR payment_count < 0;
        """,
    ),
    (
        "analytics.fact_orders.payment_total_value",
        """
        SELECT COUNT(*)
        FROM analytics.fact_orders
        WHERE payment_total_value IS NULL
           OR payment_total_value < 0;
        """,
    ),
    (
        "analytics.fact_order_items.price",
        """
        SELECT COUNT(*)
        FROM analytics.fact_order_items
        WHERE price IS NULL
           OR price < 0;
        """,
    ),
    (
        "analytics.fact_order_items.freight_value",
        """
        SELECT COUNT(*)
        FROM analytics.fact_order_items
        WHERE freight_value IS NULL
           OR freight_value < 0;
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
                invalid_count = cur.fetchone()[0]

                print(
                    f"{check_name}: "
                    f"invalid_count={invalid_count}"
                )

                if invalid_count != 0:
                    failures.append(
                        f"{check_name}: "
                        f"found {invalid_count} invalid values"
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