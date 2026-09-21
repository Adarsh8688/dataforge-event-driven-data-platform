from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_ROOT = "/mnt/c/Users/mamid/OneDrive/Documents/dataforge-event-driven-data-platform"


with DAG(
    dag_id="dataforge_pipeline",
    start_date=datetime(2026, 9, 20),
    schedule=None,
    catchup=False,
    tags=["dataforge"],
) as dag:

    load_dim_customer = BashOperator(
        task_id="load_dim_customer",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "spark-submit scripts/load_dim_customer.py"
        ),
    )

    load_dim_product = BashOperator(
        task_id="load_dim_product",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "spark-submit scripts/load_dim_product.py"
        ),
    )

    load_dim_date = BashOperator(
        task_id="load_dim_date",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "spark-submit scripts/load_dim_date.py"
        ),
    )

    load_fact_orders = BashOperator(
        task_id="load_fact_orders",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "spark-submit --jars /tmp/postgresql-42.7.7.jar "
            "scripts/load_fact_orders.py"
        ),
    )

    load_fact_order_items = BashOperator(
        task_id="load_fact_order_items",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "spark-submit --jars /tmp/postgresql-42.7.7.jar "
            "scripts/load_fact_order_items.py"
        ),
    )

    load_dim_customer >> load_dim_product >> load_dim_date
    load_dim_date >> load_fact_orders >> load_fact_order_items