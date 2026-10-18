#!/usr/bin/env bash
set -e


psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB"  \
  --set=replication_user="$REPLICATION_USER" \
  --set=replication_password="$REPLICATION_PASSWORD" <<'SQL'
CREATE ROLE :"replication_user" WITH REPLICATION LOGIN PASSWORD :'replication_password';
SQL

echo "Replication was created."
