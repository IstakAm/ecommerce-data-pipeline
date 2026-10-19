
import csv
from decimal import Decimal
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook

SCHEMA_FILE = Path("/opt/airflow/sql/01-ecommerce.sql")
INCOMING_DIR = Path("/opt/airflow/incoming")

COLUMNS = {
    "users": {"user_id", "name", "email", "created_at"},
    "products": {"product_id", "name", "category", "price"},
    "orders": {"order_id", "user_id", "order_date", "status"},
    "order_items": {
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
    },
}


def create_tables(connection):
    """Create the ecommerce tables using the shared SQL schema."""
    schema_sql = SCHEMA_FILE.read_text(encoding="utf-8")

    with connection.cursor() as cursor:
        cursor.execute(schema_sql)

    connection.commit()


def get_file_type(path):
    """Determine the entity type from the CSV filename."""
    if path.name.startswith("order_items_"):
        return "order_items"

    return path.name.split("_", 1)[0]


def read_csv(path, file_type):
    """Read a CSV file and validate its required columns and contents."""
    with path.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)

        if not reader.fieldnames:
            raise ValueError(f"{path.name}: CSV has no header.")

        missing = COLUMNS[file_type] - set(reader.fieldnames)

        if missing:
            raise ValueError(
                f"{path.name}: Missing columns: {sorted(missing)}"
            )

        rows = list(reader)

    if not rows:
        raise ValueError(f"{path.name}: CSV contains no data rows.")

    return rows


def ingest_file(connection, path, file_type):
    """Insert or update records from one CSV file."""
    rows = read_csv(path, file_type)

    with connection.cursor() as cursor:
        if file_type == "users":
            values = [
                (
                    row["user_id"],
                    row["name"],
                    row["email"],
                    row["created_at"],
                )
                for row in rows
            ]

            cursor.executemany("""
                INSERT INTO users
                    (user_id, name, email, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET
		    name = EXCLUDED.name,
		    email = EXCLUDED.email,
                    created_at = EXCLUDED.created_at
            """, values)

        elif file_type == "products":
            values = [
                (
                    row["product_id"],
                    row["name"],
                    row["category"],
                    Decimal(row["price"]),
                )
                for row in rows
            ]

            cursor.executemany("""
                INSERT INTO products
                    (product_id, name, category, price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    price = EXCLUDED.price
            """, values)

        elif file_type == "orders":
            values = [
                (
                    row["order_id"],
                    row["user_id"],
                    row["order_date"],
                    row["status"],
                )
                for row in rows
            ]

            cursor.executemany("""
                INSERT INTO ecommerce_orders
                    (order_id, user_id, order_date, status)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE SET
   		    user_id = EXCLUDED.user_id,
   		    order_date = EXCLUDED.order_date,
                    status = EXCLUDED.status
            """, values)

        elif file_type == "order_items":
            values = [
                (
                    row["order_id"],
                    row["product_id"],
                    int(row["quantity"]),
                    Decimal(row["unit_price"]),
                )
                for row in rows
            ]

            cursor.executemany("""
                INSERT INTO ecommerce_order_items
                    (order_id, product_id, quantity, unit_price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (order_id, product_id) DO UPDATE SET
   	            quantity = EXCLUDED.quantity,
                    unit_price = EXCLUDED.unit_price
            """, values)

        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    connection.commit()

    print(f"Loaded {len(rows)} rows from {path.name}")


def ingest_ecommerce_files(file_type):
    """
    Ingest CSV files for one entity.

    Called by the individual Airflow DAGs with one of:
    users, products, orders, order_items.
    """
    if file_type not in COLUMNS:
        raise ValueError(f"Unsupported file type: {file_type}")

    processed_dir = INCOMING_DIR / "processed"
    failed_dir = INCOMING_DIR / "failed"

    processed_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)

    hook = PostgresHook(postgres_conn_id="postgres_primary")
    connection = hook.get_conn()

    try:
        create_tables(connection)

        files = sorted(
            path
            for path in INCOMING_DIR.glob("*.csv")
            if get_file_type(path) == file_type
        )

        if not files:
            print(f"No new {file_type} CSV files found.")
            return

        for path in files:
            try:
                ingest_file(connection, path, file_type)
                path.rename(processed_dir / path.name)

            except Exception:
                connection.rollback()

                if path.exists():
                    path.rename(failed_dir / path.name)

                raise

        with connection.cursor() as cursor:
            table_names = {
                "users": "users",
                "products": "products",
                "orders": "ecommerce_orders",
                "order_items": "ecommerce_order_items",
            }

            cursor.execute(
                f"SELECT COUNT(*) FROM {table_names[file_type]}"
            )
            total = cursor.fetchone()[0]

        print(
            f"Database total — {file_type}: {total}"
        )

    finally:
        connection.close()
