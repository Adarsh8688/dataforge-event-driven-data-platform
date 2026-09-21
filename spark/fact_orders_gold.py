from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


# ============================================================
# Project paths
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_PATH = PROJECT_ROOT / "data" / "silver"
GOLD_PATH = PROJECT_ROOT / "data" / "gold" / "fact_orders"


# ============================================================
# Spark session
# ============================================================

spark = (
    SparkSession.builder
    .appName("dataforge-fact-orders-gold")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)


# ============================================================
# Read Silver layer
# ============================================================

orders = spark.read.parquet(
    str(SILVER_PATH / "orders")
)

customers = spark.read.parquet(
    str(SILVER_PATH / "customers")
)

order_items = spark.read.parquet(
    str(SILVER_PATH / "order_items")
)

payments = spark.read.parquet(
    str(SILVER_PATH / "payments")
)


# ============================================================
# Financial data type
# ============================================================

MONEY_TYPE = DecimalType(18, 2)

ZERO_MONEY = F.lit(0).cast(MONEY_TYPE)


# ============================================================
# Aggregate order items
# ============================================================

item_metrics = (
    order_items
    .groupBy("order_id")
    .agg(
        F.count("*").alias("item_count"),

        F.countDistinct("product_id").alias(
            "product_count"
        ),

        F.round(
            F.sum(
                F.coalesce(
                    F.col("price"),
                    ZERO_MONEY
                )
            ),
            2
        ).cast(MONEY_TYPE).alias(
            "merchandise_value"
        ),

        F.round(
            F.sum(
                F.coalesce(
                    F.col("freight_value"),
                    ZERO_MONEY
                )
            ),
            2
        ).cast(MONEY_TYPE).alias(
            "freight_value"
        ),
    )
)


# ============================================================
# Aggregate payments
# ============================================================

payment_metrics = (
    payments
    .groupBy("order_id")
    .agg(
        F.count("*").alias(
            "payment_count"
        ),

        F.round(
            F.sum(
                F.coalesce(
                    F.col("payment_value"),
                    ZERO_MONEY
                )
            ),
            2
        ).cast(MONEY_TYPE).alias(
            "payment_total_value"
        ),
    )
)


# ============================================================
# Convert Unix epoch MICROSECONDS to timestamps
# ============================================================

def epoch_us_to_timestamp(column_name):
    """
    Convert Unix epoch microseconds into a Spark timestamp.

    The CDC Silver layer stores these timestamp values
    as 16-digit epoch microsecond values.

    Null input remains null.
    """
    return F.when(
        F.col(column_name).isNotNull(),
        F.timestamp_micros(
            F.col(column_name)
        ),
    )


# ============================================================
# Prepare orders
# ============================================================

orders_prepared = (
    orders

    .withColumn(
        "order_purchase_timestamp",
        epoch_us_to_timestamp(
            "order_purchase_timestamp"
        ),
    )

    .withColumn(
        "order_approved_at",
        epoch_us_to_timestamp(
            "order_approved_at"
        ),
    )

    .withColumn(
        "order_delivered_carrier_date",
        epoch_us_to_timestamp(
            "order_delivered_carrier_date"
        ),
    )

    .withColumn(
        "order_delivered_customer_date",
        epoch_us_to_timestamp(
            "order_delivered_customer_date"
        ),
    )

    .withColumn(
        "order_estimated_delivery_date",
        epoch_us_to_timestamp(
            "order_estimated_delivery_date"
        ),
    )
)


# ============================================================
# Build Gold fact_orders
# ============================================================

fact_orders = (
    orders_prepared.alias("o")

    # --------------------------------------------------------
    # Customer enrichment
    # --------------------------------------------------------

    .join(
        customers.alias("c"),
        F.col("o.customer_id")
        == F.col("c.customer_id"),
        "left",
    )

    # --------------------------------------------------------
    # Order-item metrics
    # --------------------------------------------------------

    .join(
        item_metrics.alias("i"),
        F.col("o.order_id")
        == F.col("i.order_id"),
        "left",
    )

    # --------------------------------------------------------
    # Payment metrics
    # --------------------------------------------------------

    .join(
        payment_metrics.alias("p"),
        F.col("o.order_id")
        == F.col("p.order_id"),
        "left",
    )

    # --------------------------------------------------------
    # Select final Gold columns
    # --------------------------------------------------------

    .select(

        # ----------------------------------------------------
        # Order identifiers
        # ----------------------------------------------------

        F.col("o.order_id").alias(
            "order_id"
        ),

        F.col("o.customer_id").alias(
            "customer_id"
        ),

        # ----------------------------------------------------
        # Customer attributes
        # ----------------------------------------------------

        F.col("c.customer_unique_id").alias(
            "customer_unique_id"
        ),

        F.col("c.customer_city").alias(
            "customer_city"
        ),

        F.col("c.customer_state").alias(
            "customer_state"
        ),

        # ----------------------------------------------------
        # Order attributes
        # ----------------------------------------------------

        F.col("o.order_status").alias(
            "order_status"
        ),

        # ----------------------------------------------------
        # Order timestamps
        # ----------------------------------------------------

        F.col(
            "o.order_purchase_timestamp"
        ).alias(
            "order_purchase_timestamp"
        ),

        F.col(
            "o.order_approved_at"
        ).alias(
            "order_approved_at"
        ),

        F.col(
            "o.order_delivered_carrier_date"
        ).alias(
            "order_delivered_carrier_date"
        ),

        F.col(
            "o.order_delivered_customer_date"
        ).alias(
            "order_delivered_customer_date"
        ),

        F.col(
            "o.order_estimated_delivery_date"
        ).alias(
            "order_estimated_delivery_date"
        ),

        # ----------------------------------------------------
        # Order-item metrics
        # ----------------------------------------------------

        F.coalesce(
            F.col("i.item_count"),
            F.lit(0),
        ).alias(
            "item_count"
        ),

        F.coalesce(
            F.col("i.product_count"),
            F.lit(0),
        ).alias(
            "product_count"
        ),

        F.coalesce(
            F.col("i.merchandise_value"),
            ZERO_MONEY,
        ).alias(
            "merchandise_value"
        ),

        F.coalesce(
            F.col("i.freight_value"),
            ZERO_MONEY,
        ).alias(
            "freight_value"
        ),

        # ----------------------------------------------------
        # Total order value
        # ----------------------------------------------------

        F.round(
            F.coalesce(
                F.col("i.merchandise_value"),
                ZERO_MONEY,
            )
            +
            F.coalesce(
                F.col("i.freight_value"),
                ZERO_MONEY,
            ),
            2,
        ).cast(MONEY_TYPE).alias(
            "order_total_value"
        ),

        # ----------------------------------------------------
        # Payment metrics
        # ----------------------------------------------------

        F.coalesce(
            F.col("p.payment_count"),
            F.lit(0),
        ).alias(
            "payment_count"
        ),

        F.coalesce(
            F.col("p.payment_total_value"),
            ZERO_MONEY,
        ).alias(
            "payment_total_value"
        ),
    )

    # ========================================================
    # Delivery metrics
    # ========================================================

    .withColumn(
        "delivery_days",
        F.when(
            F.col(
                "order_delivered_customer_date"
            ).isNotNull()
            &
            F.col(
                "order_purchase_timestamp"
            ).isNotNull(),

            F.round(
                (
                    F.col(
                        "order_delivered_customer_date"
                    ).cast("long")
                    -
                    F.col(
                        "order_purchase_timestamp"
                    ).cast("long")
                )
                / F.lit(86400.0),
                2,
            ),
        ),
    )

    .withColumn(
        "estimated_delivery_days",
        F.when(
            F.col(
                "order_estimated_delivery_date"
            ).isNotNull()
            &
            F.col(
                "order_purchase_timestamp"
            ).isNotNull(),

            F.round(
                (
                    F.col(
                        "order_estimated_delivery_date"
                    ).cast("long")
                    -
                    F.col(
                        "order_purchase_timestamp"
                    ).cast("long")
                )
                / F.lit(86400.0),
                2,
            ),
        ),
    )

    .withColumn(
        "delivery_delay_days",
        F.when(
            F.col(
                "order_delivered_customer_date"
            ).isNotNull()
            &
            F.col(
                "order_estimated_delivery_date"
            ).isNotNull(),

            F.round(
                (
                    F.col(
                        "order_delivered_customer_date"
                    ).cast("long")
                    -
                    F.col(
                        "order_estimated_delivery_date"
                    ).cast("long")
                )
                / F.lit(86400.0),
                2,
            ),
        ),
    )
)


# ============================================================
# Write Gold layer
# ============================================================

fact_orders.write \
    .mode("overwrite") \
    .parquet(str(GOLD_PATH))


# ============================================================
# Validation
# ============================================================

fact_count = fact_orders.count()

print(
    f"Gold fact_orders count: {fact_count}"
)


# ============================================================
# Duplicate order ID validation
# ============================================================

duplicate_count = (
    fact_orders
    .groupBy("order_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(
    f"Duplicate order IDs: {duplicate_count}"
)


# ============================================================
# Null order ID validation
# ============================================================

null_order_id_count = (
    fact_orders
    .filter(
        F.col("order_id").isNull()
    )
    .count()
)

print(
    f"Null order IDs: {null_order_id_count}"
)


# ============================================================
# Print schema
# ============================================================

print(
    "Gold fact_orders schema:"
)

fact_orders.printSchema()


# ============================================================
# Stop Spark
# ============================================================

spark.stop()