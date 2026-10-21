"""Kafka consumer that writes messages to ClickHouse.

DEPRECATED — replaced by ClickHouse Kafka engine + Materialized View.

See clickhouse-init/02-kafka-engine.sql for the replacement.

This file is kept for reference only. Do not run it.
The ClickHouse Kafka engine handles Kafka → ClickHouse natively,
without a Python consumer, and ClickHouse manages offsets internally.


Long-running service. Run as a container, NOT as an Airflow DAG.

Semantics:
  - Manual offset commit: commit AFTER ClickHouse confirms the insert.
  - At-least-once: on crash, re-consumed messages are re-inserted.
  - ClickHouse ReplacingMergeTree deduplicates by primary key at merge time.
  - Graceful shutdown on SIGTERM/SIGINT: commit offsets, close client.
"""

import json
import os
import signal
from datetime import datetime

import clickhouse_connect
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID  = os.environ.get("KAFKA_GROUP_ID", "clickhouse-consumer")
TOPICS    = os.environ.get(
    "KAFKA_TOPICS",
    "ecommerce.users,ecommerce.orders,ecommerce.order_items",
).split(",")

CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CH_DB   = os.environ.get("CLICKHOUSE_DATABASE", "ecommerce")
CH_USER = os.environ.get("CLICKHOUSE_USER", "default")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "")

# topic -> (clickhouse_table, columns_in_insert_order)
TABLE_MAP = {
    "ecommerce.users": (
        "users",
        ["user_id", "name", "email", "created_at"],
    ),
    "ecommerce.orders": (
        "orders",
        ["order_id", "user_id", "order_date", "status"],
    ),
    "ecommerce.order_items": (
        "order_items",
        ["order_id", "product_id", "quantity", "unit_price"],
    ),
}

# Which columns per topic are timestamp columns (DateTime64 in ClickHouse).
TIMESTAMP_COLS = {
    "ecommerce.users":       ["created_at"],
    "ecommerce.orders":      ["order_date"],
    "ecommerce.order_items": [],
}


_running = True


def _shutdown(signum, _frame):
    global _running
    print(f"Received signal {signum}; shutting down...", flush=True)
    _running = False


signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT,  _shutdown)


def _make_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers":        BOOTSTRAP,
        "group.id":                 GROUP_ID,
        "auto.offset.reset":        "earliest",
        "enable.auto.commit":       False,
        "enable.auto.offset.store": False,
        "session.timeout.ms":       30000,
        "max.poll.interval.ms":     300000,
    })


def _make_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASS,
        database=CH_DB,
    )


def _parse_ts(value):
    """Convert ISO-8601 string to timezone-aware datetime.

    ClickHouse DateTime64 columns require Python datetime objects;
    the Kafka payload carries timestamps as JSON strings.
    """
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            if value.endswith("Z"):
                return datetime.fromisoformat(value[:-1] + "+00:00")
            raise
    return value


def _insert_record(client, topic: str, record: dict) -> None:
    """Insert one record into the ClickHouse table for the topic."""
    table, cols = TABLE_MAP[topic]
    ts_cols = set(TIMESTAMP_COLS.get(topic, []))

    row = []
    for c in cols:
        val = record.get(c)
        if c in ts_cols:
            val = _parse_ts(val)
        row.append(val)

    client.insert(table, [row], column_names=cols)


def main() -> None:
    consumer = _make_consumer()
    consumer.subscribe(TOPICS)
    client = _make_client()

    # sanity: verify tables exist
    for topic, (table, _cols) in TABLE_MAP.items():
        result = client.query(f"EXISTS TABLE {CH_DB}.{table}")
        exists = result.result_rows[0][0] if result.result_rows else 0
        if not exists:
            raise RuntimeError(
                f"ClickHouse table {CH_DB}.{table} does not exist. "
                f"Run the schema init (clickhouse-init/01-schema.sql) first."
            )

    print(
        f"Consuming {TOPICS} as group={GROUP_ID}; "
        f"writing to ClickHouse {CH_HOST}:{CH_PORT}/{CH_DB}",
        flush=True,
    )

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}", flush=True)
                continue

            topic = msg.topic()
            if topic not in TABLE_MAP:
                print(f"Unexpected topic {topic}; skipping", flush=True)
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))
                _insert_record(client, topic, record)

                consumer.store_offsets(message=msg)
                consumer.commit(asynchronous=False)

                print(
                    f"[{topic}] inserted 1 row "
                    f"(partition={msg.partition()} offset={msg.offset()})",
                    flush=True,
                )
            except Exception as e:
                print(
                    f"[{topic}] insert failed "
                    f"(partition={msg.partition()} offset={msg.offset()}): {e}",
                    flush=True,
                )

    finally:
        try:
            consumer.commit(asynchronous=False)
        except Exception as e:
            print(f"Final commit failed: {e}", flush=True)
        client.close()
        consumer.close()
        print("Consumer closed cleanly.", flush=True)


if __name__ == "__main__":
    main()
