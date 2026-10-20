import csv
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


INCOMING_DIR = Path("/opt/airflow/incoming")

COLUMNS = {
    "users": {"user_id", "name", "email", "created_at"},
    "products": {"product_id", "name", "category", "price"},
    "orders": {"order_id", "user_id", "order_date", "status"},
    "order_items": {
        "order_id", "product_id", "quantity", "unit_price"
    },
}


def create_tables(connection):
    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id UUID PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                created_at TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS products (
                product_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price NUMERIC(10, 2) NOT NULL CHECK (price >= 0)
            );

            CREATE TABLE IF NOT EXISTS ecommerce_orders (
                order_id UUID PRIMARY KEY,
                user_id UUID NOT NULL REFERENCES users(user_id),
                order_date TIMESTAMPTZ NOT NULL,
                status TEXT NOT NULL
                    CHECK (status IN ('pending', 'paid', 'shipped'))
            );

            CREATE TABLE IF NOT EXISTS ecommerce_order_items (
                order_id UUID NOT NULL
                    REFERENCES ecommerce_orders(order_id),
                product_id TEXT NOT NULL
                    REFERENCES products(product_id),
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                unit_price NUMERIC(10, 2) NOT NULL
                    CHECK (unit_price >= 0),
                PRIMARY KEY (order_id, product_id)
            );
        """)
    connection.commit()


def get_file_type(path):
    if path.name.startswith("order_items_"):
        return "order_items"
    return path.name.split("_", 1)[0]


def read_csv(path, file_type):
    with path.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)

        if not reader.fieldnames:
            raise ValueError("CSV has no header.")

        missing = COLUMNS[file_type] - set(reader.fieldnames)
        if missing:
            raise ValueError(f"Missing columns: {sorted(missing)}")

        rows = list(reader)

    if not rows:
        raise ValueError("CSV contains no data rows.")

    return rows


def ingest_file(connection, path, file_type):
    rows = read_csv(path, file_type)

    with connection.cursor() as cursor:
        if file_type == "users":
            values = [
                (r["user_id"], r["name"], r["email"], r["created_at"])
                for r in rows
            ]
            cursor.executemany("""
                INSERT INTO users (user_id, name, email, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id) DO NOTHING
            """, values)

        elif file_type == "products":
            values = [
                (r["product_id"], r["name"], r["category"],
                 Decimal(r["price"]))
                for r in rows
            ]
            cursor.executemany("""
                INSERT INTO products (product_id, name, category, price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    price = EXCLUDED.price
            """, values)

        elif file_type == "orders":
            values = [
                (r["order_id"], r["user_id"], r["order_date"], r["status"])
                for r in rows
            ]
            cursor.executemany("""
                INSERT INTO ecommerce_orders
                    (order_id, user_id, order_date, status)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
            """, values)

        elif file_type == "order_items":
            values = [
                (r["order_id"], r["product_id"], int(r["quantity"]),
                 Decimal(r["unit_price"]))
                for r in rows
            ]
            cursor.executemany("""
                INSERT INTO ecommerce_order_items
                    (order_id, product_id, quantity, unit_price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (order_id, product_id) DO NOTHING
            """, values)

    connection.commit()
    print(f"Loaded {len(rows)} rows from {path.name}")

def ingest_ecommerce_files(file_type):
    if file_type not in COLUMNS:
        raise ValueError(f"Unsupported file type: {file_type}")

    processed_dir = INCOMING_DIR / "processed"
    failed_dir = INCOMING_DIR / "failed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)

    hook = PostgresHook(postgres_conn_id="postgres_primary")
    connection = hook.get_conn()

    try:
        # Create schema once via bootstrap — see note below.
        # create_tables(connection)

        # Only files for THIS type, sorted oldest-first.
        candidates = sorted(
            p for p in INCOMING_DIR.glob("*.csv")
            if get_file_type(p) == file_type
        )
        if not candidates:
            print(f"No new {file_type} CSV files found.")
            return

        # Process only the OLDEST one. Its batch_id is shared with the
        # other three types, so the next child DAG will pick the same batch.
        path = candidates[0]

        try:
            ingest_file(connection, path, file_type)
            path.rename(processed_dir / path.name)
        except Exception:
            connection.rollback()
            if path.exists():
                path.rename(failed_dir / path.name)
            raise

        with connection.cursor() as cursor:
            table = {
                "users": "users", "products": "products",
                "orders": "ecommerce_orders", "order_items": "ecommerce_order_items",
            }[file_type]
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total = cursor.fetchone()[0]

        print(f"Database total — {file_type}: {total}")

    finally:
        connection.close()

with DAG(
    dag_id="ingest_ecommerce_files",
    start_date=datetime(2026, 10, 17, tzinfo=timezone.utc),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "ingestion"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_ecommerce_files",
        python_callable=ingest_ecommerce_files,
    )
