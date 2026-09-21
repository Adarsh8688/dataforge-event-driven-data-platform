from pathlib import Path

import pandas as pd


DATA_DIR = Path("data/raw")


FILES = [
    "olist_customers_dataset.csv",
    "olist_products_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
]


for file_name, table_name in TABLES.items():

    file_path = DATA_DIR / file_name

    print(f"Loading {file_name}...")

    df = pd.read_csv(file_path)

    if table_name == "products":
        df = df.rename(
            columns={
                "product_name_lenght": "product_name_length",
                "product_description_lenght": "product_description_length",
            }
        )

    if table_name == "orders":
        timestamp_columns = [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ]

        for column in timestamp_columns:
            df[column] = pd.to_datetime(
                df[column],
                errors="coerce"
            )

    if table_name == "order_items":
        df["shipping_limit_date"] = pd.to_datetime(
            df["shipping_limit_date"],
            errors="coerce"
        )

    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        chunksize=5000,
    )

    print(f"Loaded {len(df)} rows into {table_name}")