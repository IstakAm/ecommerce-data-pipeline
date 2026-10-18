from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def check_replica():
    hook = PostgresHook(postgres_conn_id="postgres_replica")

    result = hook.get_first(
        """
        SELECT
            pg_is_in_recovery(),
            current_database(),
            current_timestamp;
        """
    )

    is_replica, database_name, server_time = result

    print(f"Database: {database_name}")
    print(f"Is standby replica: {is_replica}")
    print(f"PostgreSQL server time: {server_time}")

    if not is_replica:
        raise ValueError("The connected server is not a standby replica.")


with DAG(
    dag_id="check_postgres_replica",
    start_date=datetime(2026, 10, 17),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["postgres", "learning"],
) as dag:
    check = PythonOperator(
        task_id="check_replica",
        python_callable=check_replica,
    )
