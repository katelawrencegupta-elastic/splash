#!/bin/sh
# Prepare Elastic remote_write URL + ApiKey credential, then exec Prometheus.
set -eu

SECRET_DIR="${SECRET_DIR:-/etc/prometheus/secrets}"
SECRET_FILE="${SECRET_DIR}/elastic_api_key"
CONFIG_TMPL="${CONFIG_TMPL:-/etc/prometheus/prometheus.yml.tmpl}"
CONFIG_OUT="${CONFIG_OUT:-/tmp/prometheus.yml}"

strip() {
  printf '%s' "$1" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
}

# Prefer a metrics-scoped key; fall back to ingest key with a warning.
if [ -n "${PROMETHEUS_ELASTIC_API_KEY:-}" ]; then
  RAW_KEY="$(strip "$PROMETHEUS_ELASTIC_API_KEY")"
  KEY_SOURCE=PROMETHEUS_ELASTIC_API_KEY
elif [ -n "${ELASTIC_API_KEY:-}" ]; then
  RAW_KEY="$(strip "$ELASTIC_API_KEY")"
  KEY_SOURCE=ELASTIC_API_KEY
  echo "WARNING: using ELASTIC_API_KEY for remote_write; prefer PROMETHEUS_ELASTIC_API_KEY with metrics-* privileges" >&2
else
  echo "PROMETHEUS_ELASTIC_API_KEY (or ELASTIC_API_KEY) is required for remote_write" >&2
  exit 1
fi

if [ -z "$RAW_KEY" ]; then
  echo "$KEY_SOURCE is empty after stripping whitespace" >&2
  exit 1
fi

# Prefer explicit override; otherwise derive from the same ELASTIC_HOST as ingest.
if [ -n "${PROMETHEUS_REMOTE_WRITE_URL:-}" ]; then
  REMOTE_WRITE_URL="$(strip "$PROMETHEUS_REMOTE_WRITE_URL")"
  if [ -z "$REMOTE_WRITE_URL" ]; then
    echo "PROMETHEUS_REMOTE_WRITE_URL is empty after stripping whitespace" >&2
    exit 1
  fi
else
  if [ -z "${ELASTIC_HOST:-}" ]; then
    echo "ELASTIC_HOST (or PROMETHEUS_REMOTE_WRITE_URL) is required for remote_write" >&2
    exit 1
  fi
  HOST="$(strip "$ELASTIC_HOST" | sed 's|/*$||')"
  if [ -z "$HOST" ]; then
    echo "ELASTIC_HOST is empty after stripping whitespace" >&2
    exit 1
  fi
  REMOTE_WRITE_URL="${HOST}/_prometheus/api/v1/write"
fi

case "$REMOTE_WRITE_URL" in
  http://*|https://*) ;;
  *)
    echo "remote_write URL must start with http:// or https://, got: $REMOTE_WRITE_URL" >&2
    exit 1
    ;;
esac

SHARD_ID="$(strip "${SPLASH_SHARD_ID:-0}")"
case "$SHARD_ID" in
  ''|*[!0-9]*)
    echo "SPLASH_SHARD_ID must be a non-negative integer, got: ${SPLASH_SHARD_ID:-}" >&2
    exit 1
    ;;
esac

if [ ! -f "$CONFIG_TMPL" ]; then
  echo "prometheus config template missing: $CONFIG_TMPL" >&2
  exit 1
fi

# Escape & \ for sed replacement; values must not contain newlines.
ESCAPED_URL=$(printf '%s' "$REMOTE_WRITE_URL" | sed -e 's/[&\\|]/\\&/g')
ESCAPED_SHARD=$(printf '%s' "$SHARD_ID" | sed -e 's/[&\\|]/\\&/g')
sed \
  -e "s|__REMOTE_WRITE_URL__|${ESCAPED_URL}|g" \
  -e "s|__SHARD_ID__|${ESCAPED_SHARD}|g" \
  "$CONFIG_TMPL" >"$CONFIG_OUT"

mkdir -p "$SECRET_DIR"

# Elastic Authorization: ApiKey <base64(id:api_key)>. If the value already
# looks base64 (no colon), use as-is; otherwise encode id:secret.
case "$RAW_KEY" in
  *:*)
    printf '%s' "$RAW_KEY" | base64 | tr -d '\n' >"$SECRET_FILE"
    ;;
  *)
    printf '%s' "$RAW_KEY" >"$SECRET_FILE"
    ;;
esac
chmod 600 "$SECRET_FILE"

echo "prometheus remote_write -> $REMOTE_WRITE_URL (auth=$KEY_SOURCE shard=$SHARD_ID)" >&2

exec /bin/prometheus --config.file="$CONFIG_OUT" "$@"
