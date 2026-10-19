
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

set -a
source .env
set +a
: "${DEBEZIUM_PASSWORD:?DEBEZIUM_PASSWORD must be set in .env}"

tmp_file="$(mktemp)"
config_file="$(mktemp)"
trap 'rm -f "$tmp_file" "$config_file"' EXIT
chmod 600 "$tmp_file" "$config_file"

python3 - "$DEBEZIUM_PASSWORD" \
  debezium/connector.json.template "$tmp_file" "$config_file" <<'PY'
import json
import sys

password, template_path, full_path, config_path = sys.argv[1:]

with open(template_path, encoding="utf-8") as f:
    template = f.read()

config = json.loads(template.replace("${DEBEZIUM_PASSWORD}", password))

with open(full_path, "w", encoding="utf-8") as f:
    json.dump(config, f)

with open(config_path, "w", encoding="utf-8") as f:
    json.dump(config["config"], f)
PY

base_url="http://localhost:8083"
connector_name="ecommerce-postgres-connector"

ready=false
for attempt in $(seq 1 30); do
  if curl -fsS "$base_url/connectors" >/dev/null; then
    ready=true
    break
  fi
  sleep 2
done

if [ "$ready" != true ]; then
  echo "Kafka Connect REST API did not become ready" >&2
  exit 1
fi

if curl -fsS "$base_url/connectors/$connector_name" >/dev/null; then
  curl -fsS -X PUT \
    "$base_url/connectors/$connector_name/config" \
    -H 'Content-Type: application/json' \
    --data-binary @"$config_file" >/dev/null
  echo "Connector configuration updated."
else
  curl -fsS -X POST "$base_url/connectors" \
    -H 'Content-Type: application/json' \
    --data-binary @"$tmp_file" >/dev/null
  echo "Connector created."
fi

curl -fsS "$base_url/connectors/$connector_name/status"
echo
