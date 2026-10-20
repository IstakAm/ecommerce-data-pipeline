from datetime import datetime, timezone, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from ecommerce_ingestion import ingest_ecommerce_files


def _ingest_with_batch(file_type, **context):
    batch_id = (context["dag_run"].conf or {}).get("batch_id")
    if not batch_id:
        raise ValueError(
            f"{file_type}: child DAG must be triggered with "
            f"conf={{'batch_id': '<id>'}} (got {context['dag_run'].conf!r})"
        )
    ingest_ecommerce_files(file_type=file_type, batch_id=batch_id)


with DAG(
    dag_id="ingest_order_items",
    start_date=datetime(2026, 10, 17, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"execution_timeout": timedelta(minutes=7), "retries": 0},
    tags=["ecommerce", "ingestion", "order_items"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_order_items",
        python_callable=_ingest_with_batch,
        op_kwargs={"file_type": "order_items"},
    )
