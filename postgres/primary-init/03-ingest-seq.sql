-- 02: add a DB-assigned monotonic sequence to each source table.
-- The Kafka publisher uses this as its watermark column:
-- rows with ingest_seq > last_published are "new, unpublished."
--
-- GENERATED ALWAYS AS IDENTITY: Postgres fills it in automatically.
-- The ingestion DAGs never supply it — INSERT statements are unchanged.

ALTER TABLE public.users
    ADD COLUMN IF NOT EXISTS ingest_seq BIGINT GENERATED ALWAYS AS IDENTITY;

ALTER TABLE public.ecommerce_orders
    ADD COLUMN IF NOT EXISTS ingest_seq BIGINT GENERATED ALWAYS AS IDENTITY;

ALTER TABLE public.ecommerce_order_items
    ADD COLUMN IF NOT EXISTS ingest_seq BIGINT GENERATED ALWAYS AS IDENTITY;

CREATE INDEX IF NOT EXISTS idx_users_ingest_seq
    ON public.users (ingest_seq);
CREATE INDEX IF NOT EXISTS idx_ecommerce_orders_ingest_seq
    ON public.ecommerce_orders (ingest_seq);
CREATE INDEX IF NOT EXISTS idx_ecommerce_order_items_ingest_seq
    ON public.ecommerce_order_items (ingest_seq);
