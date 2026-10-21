#!/usr/bin/env bash
# Run this whenever you wipe clickhouse_data and need to re-read historical Kafka.
# Sequence: stop ClickHouse -> reset consumer groups -> start ClickHouse.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "Stopping ClickHouse..."
docker compose stop clickhouse
sleep 15

echo "Resetting consumer groups to earliest..."
for pair in \
  "ecommerce.users:clickhouse-kafka-users" \
  "ecommerce.orders:clickhouse-kafka-orders" \
  "ecommerce.order_items:clickhouse-kafka-order-items" \
  "ecommerce.products:clickhouse-kafka-products"; do
  topic="${pair%%:*}"
  group="${pair##*:}"
  echo "--- $group ---"
  docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group "$group" --topic "$topic" \
    --reset-offsets --to-earliest --execute 2>&1 | grep -v "has no active members" || true
done

echo "Starting ClickHouse..."
docker compose start clickhouse

echo "Waiting for consumption to begin..."
sleep 30
echo "=== Consumer state ==="
docker exec -i clickhouse clickhouse-client --query "
  SELECT table, num_messages_read, is_currently_used, length(exceptions.time) AS err_count
  FROM system.kafka_consumers WHERE table LIKE 'kafka_%' ORDER BY table FORMAT Pretty
"
