-- 03: watermark for the Kafka publisher. One row per (pipeline, entity).
-- The publisher reads  WHERE ingest_seq > last_ingest_seq, publishes,
-- then advances last_ingest_seq.

CREATE TABLE IF NOT EXISTS public.kafka_publish_watermarks (
    pipeline_name   TEXT        NOT NULL,
    entity          TEXT        NOT NULL,
    last_ingest_seq BIGINT      NOT NULL DEFAULT 0,
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_name, entity)
);

-- Seed one row per entity at 0.
INSERT INTO public.kafka_publish_watermarks (pipeline_name, entity, last_ingest_seq)
VALUES
    ('ecommerce_to_kafka', 'users',       0),
    ('ecommerce_to_kafka', 'orders',      0),
    ('ecommerce_to_kafka', 'order_items', 0)
ON CONFLICT (pipeline_name, entity) DO NOTHING;

-- Skip the historical backlog: start the publisher from "now".
-- Remove this block if you want the publisher to re-publish everything.
UPDATE public.kafka_publish_watermarks w
SET last_ingest_seq = sub.max_seq, last_updated_at = now()
FROM (
    SELECT 'users' AS e,        COALESCE(max(ingest_seq), 0) AS max_seq FROM public.users
    UNION ALL
    SELECT 'orders',            COALESCE(max(ingest_seq), 0) FROM public.ecommerce_orders
    UNION ALL
    SELECT 'order_items',       COALESCE(max(ingest_seq), 0) FROM public.ecommerce_order_items
) sub
WHERE w.pipeline_name = 'ecommerce_to_kafka' AND w.entity = sub.e;
