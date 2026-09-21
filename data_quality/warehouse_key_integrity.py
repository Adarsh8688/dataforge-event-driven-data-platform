import psycopg2
import os

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "nordmarket",
    "user": "dataforge",
    "password": os.getenv("DATAFORGE_DB_PASSWORD"),
}


CHECKS = {
    "analytics.dim_customer": [
        ("customer_id", "customer_id IS NOT NULL"),
    ],
    "analytics.dim_product": [
        ("product_id", "product_id IS NOT NULL"),
    ],
    "analytics.fact_orders": [
        ("order_id", "order_id IS NOT NULL"),
    ],
}


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    failures = []

    try:
        with conn.cursor() as cur:
            for table_name, checks in CHECKS.items():
                for key_name, not_null_condition in checks:
                    cur.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM {table_name}
                        WHERE NOT ({not_null_condition});
                        """
                    )

                    null_count = cur.fetchone()[0]

                    print(
                        f"{table_name}.{key_name}: "
                        f"null_count={null_count}"
                    )

                    if null_count != 0:
                        failures.append(
                            f"{table_name}.{key_name}: "
                            f"found {null_count} null values"
                        )

                    cur.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM (
                            SELECT {key_name}
                            FROM {table_name}
                            GROUP BY {key_name}
                            HAVING COUNT(*) > 1
                        ) duplicates;
                        """
                    )

                    duplicate_count = cur.fetchone()[0]

                    print(
                        f"{table_name}.{key_name}: "
                        f"duplicate_key_count={duplicate_count}"
                    )

                    if duplicate_count != 0:
                        failures.append(
                            f"{table_name}.{key_name}: "
                            f"found {duplicate_count} duplicate keys"
                        )

            cur.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT order_id, order_item_id
                    FROM analytics.fact_order_items
                    GROUP BY order_id, order_item_id
                    HAVING COUNT(*) > 1
                ) duplicates;
                """
            )

            duplicate_item_keys = cur.fetchone()[0]

            print(
                "analytics.fact_order_items.(order_id, order_item_id): "
                f"duplicate_key_count={duplicate_item_keys}"
            )

            if duplicate_item_keys != 0:
                failures.append(
                    "analytics.fact_order_items.(order_id, order_item_id): "
                    f"found {duplicate_item_keys} duplicate keys"
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