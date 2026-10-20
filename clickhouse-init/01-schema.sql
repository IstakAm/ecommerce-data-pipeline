CREATE DATABASE IF NOT EXISTS ecommerce;

CREATE TABLE IF NOT EXISTS ecommerce.users
(
    user_id     UUID,
    name        String,
    email       String,
    created_at  DateTime64(6, 'UTC'),
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS ecommerce.orders
(
    order_id    UUID,
    user_id     UUID,
    order_date  DateTime64(6, 'UTC'),
    status      LowCardinality(String),
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS ecommerce.order_items
(
    order_id    UUID,
    product_id  String,
    quantity    Int32,
    unit_price  Decimal(10, 2),
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (order_id, product_id);
