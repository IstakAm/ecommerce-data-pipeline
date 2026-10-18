
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="ecommerce_ingestion_pipeline",
    start_date=datetime(2026, 10, 17, tzinfo=timezone.utc),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "orchestration"],
) as dag:

    users = TriggerDagRunOperator(
        task_id="trigger_users",
        trigger_dag_id="ingest_users",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    products = TriggerDagRunOperator(
        task_id="trigger_products",
        trigger_dag_id="ingest_products",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    orders = TriggerDagRunOperator(
        task_id="trigger_orders",
        trigger_dag_id="ingest_orders",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    order_items = TriggerDagRunOperator(
        task_id="trigger_order_items",
        trigger_dag_id="ingest_order_items",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # Users and products can run in parallel.
    users >> orders

    # Order items require both orders and products to finish.
    [orders, products] >> order_items
