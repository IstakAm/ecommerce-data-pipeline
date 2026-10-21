-- 02-kafka-engine.sql
-- ClickHouse Kafka engine + Materialized Views.
-- Reads from Kafka topics (ecommerce.users, ecommerce.orders, ecommerce.order_items)
-- and inserts into the existing target tables (ecommerce.users, .orders, .order_items)
-- via materialized views. No external consumer required.
--
-- Engine flow:
--   Kafka topic
--     -> kafka_users (Kafka engine table, format=JSONEachRow)
--     -> kafka_users_mv (Materialized View, parses strings to typed columns)
--     -> ecommerce.users (ReplacingMergeTree, dedupes by ORDER BY key)

-- =====================================================================
-- USERS
-- =====================================================================

CREATE TABLE IF NOT EXISTS ecommerce.kafka_users
(
    user_id     String,
    name        String,
    email       String,
    created_at  String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list            = 'kafka:9092',
    kafka_topic_list             = 'ecommerce.users',
    kafka_group_name             = 'clickhouse-kafka-users',
    kafka_format                 = 'JSONEachRow',
    kafka_num_consumers          = 1,
    kafka_poll_max_batch_size    = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.kafka_users_mv
TO ecommerce.users
AS
SELECT
    toUUID(user_id)                                 AS user_id,
    name,
    email,
    parseDateTime64BestEffort(created_at, 6, 'UTC') AS created_at,
    now()                                            AS ingested_at
FROM ecommerce.kafka_users;

-- =====================================================================
-- ORDERS
-- =====================================================================

CREATE TABLE IF NOT EXISTS ecommerce.kafka_orders
(
    order_id    String,
    user_id     String,
    order_date  String,
    status      String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list            = 'kafka:9092',
    kafka_topic_list             = 'ecommerce.orders',
    kafka_group_name             = 'clickhouse-kafka-orders',
    kafka_format                 = 'JSONEachRow',
    kafka_num_consumers          = 1,
    kafka_poll_max_batch_size    = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.kafka_orders_mv
TO ecommerce.orders
AS
SELECT
    toUUID(order_id)                                    AS order_id,
    toUUID(user_id)                                     AS user_id,
    parseDateTime64BestEffort(order_date, 6, 'UTC')     AS order_date,
    status,
    now()                                                AS ingested_at
FROM ecommerce.kafka_orders;

-- =====================================================================
-- ORDER ITEMS
-- =====================================================================

CREATE TABLE IF NOT EXISTS ecommerce.kafka_order_items
(
    order_id    String,
    product_id  String,
    quantity    String,
    unit_price  String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list            = 'kafka:9092',
    kafka_topic_list             = 'ecommerce.order_items',
    kafka_group_name             = 'clickhouse-kafka-order-items',
    kafka_format                 = 'JSONEachRow',
    kafka_num_consumers          = 1,
    kafka_poll_max_batch_size    = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.kafka_order_items_mv
TO ecommerce.order_items
AS
SELECT
    toUUID(order_id)                    AS order_id,
    product_id,
    toInt32(quantity)                   AS quantity,
    toDecimal64(unit_price, 2)          AS unit_price,
    now()                                AS ingested_at
FROM ecommerce.kafka_order_items;


-- =====================================================================
-- PRODUCTS
-- =====================================================================

CREATE TABLE IF NOT EXISTS ecommerce.kafka_products
(
    product_id  String,
    name        String,
    category    String,
    price       String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list            = 'kafka:9092',
    kafka_topic_list             = 'ecommerce.products',
    kafka_group_name             = 'clickhouse-kafka-products',
    kafka_format                 = 'JSONEachRow',
    kafka_num_consumers          = 1,
    kafka_poll_max_batch_size    = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.kafka_products_mv
TO ecommerce.products
AS
SELECT
    product_id,
    name,
    category,
    toDecimal64(price, 2)  AS price,
    now()                  AS ingested_at
FROM ecommerce.kafka_products;
