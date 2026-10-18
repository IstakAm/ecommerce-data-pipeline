#!/usr/bin/env bash
set -e

echo "Waiting for PostgreSQL primary..."

until pg_isready -h primary -p 5432 -U "$REPLICATION_USER"; do
    sleep 2
done

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "Initializing replica from primary..."

    export PGPASSWORD="$REPLICATION_PASSWORD"

    pg_basebackup \
        -h primary \
        -p 5432 \
        -U "$REPLICATION_USER" \
        -D "$PGDATA" \
        -Fp \
        -Xs \
        -P \
        -R \
        -C \
        -S replica_slot

    unset PGPASSWORD
    echo "Replica initialized."
fi

exec /usr/local/bin/docker-entrypoint.sh postgres
