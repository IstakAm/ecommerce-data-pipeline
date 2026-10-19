
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

set -a
source .env
set +a

: "${DEBEZIUM_PASSWORD:?DEBEZIUM_PASSWORD must be set in .env}"


docker compose exec -T \
  -e PGPASSWORD="$POSTGRES_PASSWORD" \
  primary \
  psql -v ON_ERROR_STOP=1 \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    < postgres/schema/01-ecommerce.sql


docker compose exec -T primary \
  psql -X -v ON_ERROR_STOP=1 \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -v dbz_password="$DEBEZIUM_PASSWORD" <<'SQL'

SELECT format(
    'CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD %L',
    :'dbz_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'debezium'
)
\gexec

ALTER ROLE debezium WITH LOGIN REPLICATION
    PASSWORD :'dbz_password';

GRANT CONNECT ON DATABASE dataengfirst TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;

GRANT SELECT ON
    public.users,
    public.products,
    public.ecommerce_orders,
    public.ecommerce_order_items
TO debezium;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_publication
        WHERE pubname = 'dbz_ecommerce_pub'
    ) THEN
        IF (
            SELECT count(*)
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename IN (
                  'users',
                  'products',
                  'ecommerce_orders',
                  'ecommerce_order_items'
              )
        ) <> 4 THEN
            RAISE EXCEPTION
                'Expected ecommerce tables are missing';
        END IF;

        EXECUTE 'CREATE PUBLICATION dbz_ecommerce_pub FOR TABLE
            public.users,
            public.products,
            public.ecommerce_orders,
            public.ecommerce_order_items';
    END IF;
END
$$;

SELECT pubname
FROM pg_publication
WHERE pubname = 'dbz_ecommerce_pub';

SQL
