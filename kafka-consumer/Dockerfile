FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
        confluent-kafka==2.6.0 \
        clickhouse-connect==0.8.5

COPY consumer.py /app/consumer.py
COPY consumer_clickhouse.py /app/consumer_clickhouse.py

# Default: run the file consumer. Override CMD in docker-compose
# to run the ClickHouse consumer.
CMD ["python", "-u", "/app/consumer.py"]
