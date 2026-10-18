from datetime import datetime, timezone, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from ecommerce_ingestion import ingest_ecommerce_files


with DAG(
    dag_id="ingest_products",
    start_date=datetime(2026, 10, 17, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "ingestion", "products"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_products",
        python_callable=ingest_ecommerce_files,
        op_kwargs={"file_type": "products"},
        retries=3,
        retry_delay=timedelta(minutes=1),
    )
