#!/usr/bin/env bash
# Download the ClickHouse driver for Metabase.
# Idempotent: skips if the .jar already exists and is a valid JAR (ZIP or JAR).
set -euo pipefail
cd "$(dirname "$0")/.."

PLUGINS_DIR="metabase-plugins"
DRIVER_JAR="$PLUGINS_DIR/clickhouse.metabase-driver.jar"
DRIVER_URL="https://github.com/ClickHouse/metabase-clickhouse-driver/releases/latest/download/clickhouse.metabase-driver.jar"

mkdir -p "$PLUGINS_DIR"

# Helper: is this a valid JAR (ZIP-compressed Java archive)?
is_valid_jar() {
  [ -f "$1" ] && file "$1" | grep -qE 'Zip archive|Java archive'
}

if is_valid_jar "$DRIVER_JAR"; then
  echo "✓ ClickHouse driver already present: $DRIVER_JAR"
  file "$DRIVER_JAR"
  exit 0
fi

echo "Downloading ClickHouse driver..."
curl -fsSL -o "$DRIVER_JAR" "$DRIVER_URL"

if ! is_valid_jar "$DRIVER_JAR"; then
  echo "✗ Download failed. Got:"
  file "$DRIVER_JAR"
  echo "First 200 bytes:"
  head -c 200 "$DRIVER_JAR"
  rm -f "$DRIVER_JAR"
  exit 1
fi

echo "✓ Driver downloaded: $DRIVER_JAR"
file "$DRIVER_JAR"
ls -la "$DRIVER_JAR"
