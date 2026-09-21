from monitoring.health_check import check_tcp_service
from monitoring.warehouse_health_check import TABLES


def test_expected_warehouse_tables():
    expected_tables = {
        "analytics.dim_customer",
        "analytics.dim_product",
        "analytics.dim_date",
        "analytics.fact_orders",
        "analytics.fact_order_items",
    }

    assert set(TABLES) == expected_tables


def test_postgres_service_is_reachable():
    assert check_tcp_service("PostgreSQL", "localhost", 5432) is True
    