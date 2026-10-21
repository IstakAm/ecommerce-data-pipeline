"""Publish new Postgres rows to Kafka in batches.

One DAG run = one batch:
  1. For each entity (users, orders, order_items):
       a. Read the watermark from public.kafka_publish_watermarks.
       b. SELECT rows WHERE ingest_seq > watermark ORDER BY ingest_seq LIMIT BATCH_SIZE.
       c. Publish each row to Kafka via KafkaProducerHook (confluent-kafka under the hood).
       d. Flush the producer.
       e. UPDATE watermark = max(ingest_seq) published.
  2. Exit.

Uses the 'kafka_default' Airflow connection for the broker.
"""

import json
from datetime import datetime, timezone, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

PIPELINE_NAME = "ecommerce_to_kafka"
BATCH_SIZE    = 1000
KAFKA_CONN_ID = "kafka_default"

ENTITIES = [
    ("users",       "users"),
    ("products",    "products"),
    ("orders",      "ecommerce_orders"),
    ("order_items", "ecommerce_order_items"),
]

def publish_batch(**context):
    pg = PostgresHook(postgres_conn_id="postgres_primary")
    conn = pg.get_conn()

    kafka_hook = KafkaProducerHook(kafka_config_id=KAFKA_CONN_ID)
    producer = kafka_hook.get_producer()

    summary = {}

    try:
        for entity, table in ENTITIES:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT last_ingest_seq FROM public.kafka_publish_watermarks
                    WHERE pipeline_name = %s AND entity = %s
                """, (PIPELINE_NAME, entity))
                row = cursor.fetchone()
                watermark = row[0] if row else 0

                cursor.execute(
                    f"SELECT * FROM public.{table} "
                    f"WHERE ingest_seq > %s ORDER BY ingest_seq LIMIT %s",
                    (watermark, BATCH_SIZE),
                )
                cols = [d[0] for d in cursor.description]
                rows = cursor.fetchall()

            if not rows:
                summary[entity] = {"read": 0, "watermark": watermark}
                print(f"[{entity}] no new rows (watermark={watermark})", flush=True)
                continue

            topic = f"ecommerce.{entity}"
            last_seq = watermark
            for row in rows:
                rec = dict(zip(cols, row))
                seq = rec.pop("ingest_seq")
                # confluent-kafka uses .produce(), and the value must be bytes.
                producer.produce(
                    topic,
                    key=str(seq).encode("utf-8"),
                    value=json.dumps(rec, default=str).encode("utf-8"),
                )
                last_seq = seq

            # Wait for all messages to be delivered.
            producer.flush(timeout=30)

            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE public.kafka_publish_watermarks
                    SET last_ingest_seq = %s, last_updated_at = now()
                    WHERE pipeline_name = %s AND entity = %s
                """, (last_seq, PIPELINE_NAME, entity))
            conn.commit()

            summary[entity] = {"read": len(rows), "watermark": last_seq}
            print(
                f"[{entity}] published {len(rows)} rows -> {topic} "
                f"(watermark {watermark} -> {last_seq})",
                flush=True,
            )

    finally:
        producer.flush(timeout=30)
        conn.close()

    print(f"Batch summary: {json.dumps(summary, default=str)}", flush=True)

    if not any(s["read"] > 0 for s in summary.values()):
        raise AirflowSkipException("No new rows for any entity")


with DAG(
    dag_id="kafka_publisher",
    description="Publish new Postgres rows to Kafka (one DAG run = one batch).",
    start_date=datetime(2026, 10, 17, tzinfo=timezone.utc),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["publisher", "kafka"],
) as dag:
    PythonOperator(task_id="publish_batch", python_callable=publish_batch)
