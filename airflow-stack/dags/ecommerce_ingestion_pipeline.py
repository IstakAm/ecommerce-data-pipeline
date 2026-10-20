from datetime import datetime, timezone, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

INCOMING = Path("/opt/airflow/incoming")
TYPES = ("users", "products", "orders", "order_items")


def discover_batch(**context):
    """Return the oldest batch_id that still has work in incoming/.

    A batch is "complete" when all four file types exist across
    incoming/ + processed/ (so it stays complete as children move
    their files to processed/ mid-cycle).
    """
    present: dict[str, set[str]] = {}
    for directory in (INCOMING, INCOMING / "processed"):
        if not directory.is_dir():
            continue
        for p in directory.glob("*.csv"):
            stem = p.stem
            for t in TYPES:
                if stem.startswith(t + "_"):
                    present.setdefault(stem[len(t) + 1:], set()).add(t)
                    break

    complete = sorted(b for b, ts in present.items() if ts >= set(TYPES))
    if not complete:
        raise AirflowSkipException("No complete batch in incoming/ or processed/")

    # Return the oldest complete batch that still has at least one file
    # in incoming/ (i.e. not yet fully processed).
    for b in complete:
        if list(INCOMING.glob(f"*_{b}.csv")):
            return b

    raise AirflowSkipException("All complete batches are already fully processed")


CHILD_DEFAULTS = dict(
    wait_for_completion=True,
    poke_interval=30,
    reset_dag_run=True,
    allowed_states=["success"],
    failed_states=["failed"],
    execution_timeout=timedelta(minutes=9),
    conf={"batch_id": "{{ ti.xcom_pull(task_ids='discover_batch') }}"},
)


with DAG(
    dag_id="ecommerce_ingestion_pipeline",
    start_date=datetime(2026, 10, 17, tzinfo=timezone.utc),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "orchestration"],
) as dag:

    discover = PythonOperator(
        task_id="discover_batch",
        python_callable=discover_batch,
    )

    users = TriggerDagRunOperator(
        task_id="trigger_users",
        trigger_dag_id="ingest_users",
        **CHILD_DEFAULTS,
    )
    products = TriggerDagRunOperator(
        task_id="trigger_products",
        trigger_dag_id="ingest_products",
        **CHILD_DEFAULTS,
    )
    orders = TriggerDagRunOperator(
        task_id="trigger_orders",
        trigger_dag_id="ingest_orders",
        **CHILD_DEFAULTS,
    )
    order_items = TriggerDagRunOperator(
        task_id="trigger_order_items",
        trigger_dag_id="ingest_order_items",
        **CHILD_DEFAULTS,
    )

    discover >> [users, products] >> orders >> order_items
