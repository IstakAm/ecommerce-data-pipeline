-- Metabase's metadata database.
-- Auto-created on fresh Postgres volume init.
-- For existing deployments, apply manually:
--   docker exec -i -e PGPASSWORD=postgres pg-primary psql -U postgres < postgres/primary-init/05-metabase.sql

-- 1. Create the metabase DATABASE (idempotent).
-- Postgres has no CREATE DATABASE IF NOT EXISTS, and CREATE DATABASE can't
-- run in a DO block. So we generate the statement dynamically and execute
-- it via psql's \gexec.
SELECT 'CREATE DATABASE metabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec

-- 2. Create the metabase USER (idempotent).
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'metabase') THEN
    CREATE USER metabase WITH PASSWORD 'metabase';
  END IF;
END
$$;

-- 3. Grant privileges on the database.
GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase;

-- 4. Grant schema privileges (needs to be connected to the target DB).
\c metabase
GRANT ALL ON SCHEMA public TO metabase;
