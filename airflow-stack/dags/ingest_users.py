
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from ecommerce_ingestion import ingest_ecommerce_files


with DAG(
    dag_id="ingest_users",
    start_date=datetime(2026, 10, 17, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "ingestion", "users"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_users",
        python_callable=ingest_ecommerce_files,
        op_kwargs={"file_type": "users"},
    )

