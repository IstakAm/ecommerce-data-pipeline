"""Kafka consumer: reads messages and writes them to per-topic text files.

Design:
  - Long-running service. Run as a container, NOT as an Airflow DAG.
  - Manual offset commit: commit AFTER the write + fsync succeeds.
  - At-least-once semantics: a crash mid-write re-consumes on restart.
    Downstream consumers (files, ClickHouse) must be idempotent by PK.
  - Graceful shutdown on SIGTERM/SIGINT: commit offsets, flush files, close.

To swap to ClickHouse later:
  - Replace `_write_record()` with a ClickHouse INSERT.
  - Commit the Kafka offset only after the INSERT succeeds.
  - Everything else (polling, offset management, shutdown) is unchanged.
"""

import json
import os
import signal
import sys
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

# --- Configuration (all overridable via env) ---
BOOTSTRAP   = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID    = os.environ.get("KAFKA_GROUP_ID", "text-file-consumer")
TOPICS      = os.environ.get(
    "KAFKA_TOPICS",
    "ecommerce.users,ecommerce.orders,ecommerce.order_items",
).split(",")
OUTPUT_DIR  = Path(os.environ.get("OUTPUT_DIR", "/data/consumed"))

# topic -> output filename (one file per topic)
OUTPUT_FILES = {
    "ecommerce.users":       "users.txt",
    "ecommerce.orders":      "orders.txt",
    "ecommerce.order_items": "order_items.txt",
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
        "bootstrap.servers":       BOOTSTRAP,
        "group.id":                GROUP_ID,
        "auto.offset.reset":       "earliest",      # first start reads from beginning
        "enable.auto.commit":      False,           # we commit manually
        "enable.auto.offset.store": False,          # we store offsets manually
        "session.timeout.ms":      30000,
        "max.poll.interval.ms":    300000,
    })


def _open_outputs() -> dict[str, "io.TextIOWrapper"]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return {
        topic: open(OUTPUT_DIR / fname, "a", encoding="utf-8")
        for topic, fname in OUTPUT_FILES.items()
    }


def _write_record(fh, record: dict) -> None:
    """Write one record as a JSON line. Override this for ClickHouse later."""
    fh.write(json.dumps(record, default=str) + "\n")
    fh.flush()
    os.fsync(fh.fileno())


def main() -> None:
    consumer = _make_consumer()
    consumer.subscribe(TOPICS)
    files = _open_outputs()

    print(
        f"Consuming {TOPICS} as group={GROUP_ID}; writing to {OUTPUT_DIR}",
        flush=True,
    )

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)   # blocks up to 1s: no CPU spin
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}", flush=True)
                continue

            topic = msg.topic()
            fh = files.get(topic)
            if fh is None:
                print(f"Unexpected topic {topic}; skipping", flush=True)
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))
                _write_record(fh, record)
                # Commit ONLY after the write + fsync succeeded.
                consumer.store_offsets(message=msg)
                consumer.commit(asynchronous=False)

                print(
                    f"[{topic}] wrote 1 message "
                    f"(partition={msg.partition()} offset={msg.offset()})",
                    flush=True,
                )
            except Exception as e:
                # Don't commit; the message will be re-consumed on restart.
                print(
                    f"[{topic}] failed to write message "
                    f"(partition={msg.partition()} offset={msg.offset()}): {e}",
                    flush=True,
                )
                continue

    finally:
        # Best-effort final commit and cleanup
        try:
            consumer.commit(asynchronous=False)
        except Exception as e:
            print(f"Final commit failed: {e}", flush=True)

        for topic, fh in files.items():
            try:
                fh.flush()
                os.fsync(fh.fileno())
                fh.close()
            except Exception as e:
                print(f"Failed to close {topic} file: {e}", flush=True)

        consumer.close()
        print("Consumer closed cleanly.", flush=True)


if __name__ == "__main__":
    main()
