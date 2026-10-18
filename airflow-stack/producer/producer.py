import csv
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_DIR = Path("/data/incoming")
INTERVAL_SECONDS = 120

PRODUCTS = [
    {"product_id": "SKU-001", "name": "Keyboard", "category": "Accessories", "price": 49.99},
    {"product_id": "SKU-002", "name": "Mouse", "category": "Accessories", "price": 24.99},
    {"product_id": "SKU-003", "name": "Monitor", "category": "Electronics", "price": 249.99},
    {"product_id": "SKU-004", "name": "Headset", "category": "Accessories", "price": 79.99},
    {"product_id": "SKU-005", "name": "Webcam", "category": "Electronics", "price": 59.99},
    {"product_id": "SKU-006", "name": "Laptop Stand", "category": "Furniture", "price": 39.99},
]


def publish_csv(filename, rows, columns):
    """Write a complete CSV before publishing it to the incoming folder."""
    temporary_path = OUTPUT_DIR / f".{filename}.tmp"
    final_path = OUTPUT_DIR / filename

    with temporary_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    # Atomic publication: consumers only see the final filename when complete.
    os.replace(temporary_path, final_path)
    print(f"Published {filename}: {len(rows)} rows", flush=True)


def generate_batch():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    batch_id = f"{now.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:6]}"

    # 1. Generate new users for this batch.
    users = []
    for number in range(random.randint(5, 10)):
        user_id = str(uuid.uuid4())
        users.append({
            "user_id": user_id,
            "name": f"Customer {batch_id[-6:]}-{number + 1}",
            "email": f"{user_id}@example.test",
            "created_at": now.isoformat(),
        })

    publish_csv(
        f"users_{batch_id}.csv",
        users,
        ["user_id", "name", "email", "created_at"],
    )

    # 2. Publish the product catalog. IDs remain stable across batches.
    publish_csv(
        f"products_{batch_id}.csv",
        PRODUCTS,
        ["product_id", "name", "category", "price"],
    )

    # 3. Generate orders belonging to users created in this batch.
    orders = []
    order_items = []

    for _ in range(random.randint(5, 12)):
        order_id = str(uuid.uuid4())
        user = random.choice(users)

        orders.append({
            "order_id": order_id,
            "user_id": user["user_id"],
            "order_date": now.isoformat(),
            "status": random.choice(["pending", "paid", "shipped"]),
        })

        selected_products = random.sample(
            PRODUCTS, k=random.randint(1, 4)
        )

        for product in selected_products:
            order_items.append({
                "order_id": order_id,
                "product_id": product["product_id"],
                "quantity": random.randint(1, 3),
                "unit_price": product["price"],
            })

    # Publish orders before their associated order items.
    publish_csv(
        f"orders_{batch_id}.csv",
        orders,
        ["order_id", "user_id", "order_date", "status"],
    )

    publish_csv(
        f"order_items_{batch_id}.csv",
        order_items,
        ["order_id", "product_id", "quantity", "unit_price"],
    )


if __name__ == "__main__":
    print("Starting e-commerce data producer...", flush=True)

    while True:
        try:
            generate_batch()
        except Exception as error:
            print(f"Batch generation failed: {error}", flush=True)

        time.sleep(INTERVAL_SECONDS)
