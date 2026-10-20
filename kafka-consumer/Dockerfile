FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka==2.6.0

COPY consumer.py /app/consumer.py

# -u: unbuffered stdout so logs appear immediately
CMD ["python", "-u", "/app/consumer.py"]
