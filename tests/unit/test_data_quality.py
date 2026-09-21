from data_quality.warehouse_row_counts import EXPECTED_COUNTS


def test_expected_warehouse_counts_are_positive():
    assert EXPECTED_COUNTS
    assert all(count > 0 for count in EXPECTED_COUNTS.values())


def test_expected_warehouse_tables():
    expected_tables = {
        "analytics.dim_customer",
        "analytics.dim_product",
        "analytics.dim_date",
        "analytics.fact_orders",
        "analytics.fact_order_items",
    }

    assert set(EXPECTED_COUNTS.keys()) == expected_tables