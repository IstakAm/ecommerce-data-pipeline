#!/usr/bin/env bash
# Bootstrap the entire stack from a fresh clone.
# Usage: ./scripts/bootstrap.sh
#
# Steps:
#   1. Verify .env exists (copy from .env.example)
#   2. Download the Metabase ClickHouse driver (if needed)
#   3. Start the stack (Postgres, Kafka, ClickHouse, Metabase, etc.)
#   4. Print next steps (Metabase UI wizard)
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  if [ -f .env.example ]; then
    echo "→ .env not found; copying from .env.example"
    cp .env.example .env
    echo "  → Edit .env with real passwords before proceeding"
  else
    echo "✗ No .env or .env.example — cannot proceed"
    exit 1
  fi
fi

echo "→ Downloading Metabase ClickHouse driver..."
./scripts/download-metabase-driver.sh

echo "→ Starting the stack..."
docker compose up -d

echo ""
echo "✓ Stack is up."
echo ""
echo "Next steps:"
echo "  1. Wait ~2 minutes for Metabase to finish initializing"
echo "  2. Open http://localhost:3000"
echo "  3. Complete the wizard (any email/password works)"
echo "  4. Add databases:"
echo "       ClickHouse:  host=clickhouse port=8123 db=ecommerce user=default pass=(empty)"
echo "       Postgres:    host=pg-primary port=5432 db=dataengfirst user=postgres pass=postgres"
echo "  5. See metabase/README.md for building dashboards"
