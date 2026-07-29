#!/usr/bin/env bash
# Launch one Splash horizontal shard (unique compose project + host ports).
#
# Usage:
#   ./scripts/run-shard.sh <shard_id> up --build -d
#   ./scripts/run-shard.sh 0 ps
#   ./scripts/run-shard.sh 1 down
#
# Env overrides (optional):
#   INGEST_BIND          default 127.0.0.1 (use 0.0.0.0 for remote Splunk)
#   SHARD_PORT_STRIDE    default 10
#   COMPOSE_PROJECT_NAME default splash${SHARD_ID}
#   COOKED_HOST_PORT / UNCOOKED_HOST_PORT / CLASSIFY_HOST_PORT /
#     S2S_HEALTH_HOST_PORT / LS_API_HOST_PORT /
#     PROMETHEUS_HOST_PORT / DLQ_EXPORTER_HOST_PORT
#     (skip auto offset if cooked+uncooked set)
#   ENV_FILE             default ../.env if present, else .env
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <shard_id> [docker compose args...]" >&2
  echo "example: $0 0 up --build -d" >&2
  exit 2
fi

SHARD_ID="$1"
shift

if ! [[ "$SHARD_ID" =~ ^[0-9]+$ ]]; then
  echo "shard_id must be a non-negative integer, got: $SHARD_ID" >&2
  exit 2
fi

STRIDE="${SHARD_PORT_STRIDE:-10}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-splash${SHARD_ID}}"
export INGEST_BIND="${INGEST_BIND:-127.0.0.1}"
export SPLASH_SHARD_ID="$SHARD_ID"

if [[ -z "${COOKED_HOST_PORT:-}" || -z "${UNCOOKED_HOST_PORT:-}" ]]; then
  export UNCOOKED_HOST_PORT=$((39997 + SHARD_ID * STRIDE))
  export COOKED_HOST_PORT=$((39998 + SHARD_ID * STRIDE))
  export CLASSIFY_HOST_PORT=$((8080 + SHARD_ID * STRIDE))
  export S2S_HEALTH_HOST_PORT=$((8081 + SHARD_ID * STRIDE))
  export LS_API_HOST_PORT=$((9600 + SHARD_ID * STRIDE))
  export PROMETHEUS_HOST_PORT=$((9090 + SHARD_ID * STRIDE))
  export DLQ_EXPORTER_HOST_PORT=$((9102 + SHARD_ID * STRIDE))
else
  export CLASSIFY_HOST_PORT="${CLASSIFY_HOST_PORT:-$((8080 + SHARD_ID * STRIDE))}"
  export S2S_HEALTH_HOST_PORT="${S2S_HEALTH_HOST_PORT:-$((8081 + SHARD_ID * STRIDE))}"
  export LS_API_HOST_PORT="${LS_API_HOST_PORT:-$((9600 + SHARD_ID * STRIDE))}"
  export PROMETHEUS_HOST_PORT="${PROMETHEUS_HOST_PORT:-$((9090 + SHARD_ID * STRIDE))}"
  export DLQ_EXPORTER_HOST_PORT="${DLQ_EXPORTER_HOST_PORT:-$((9102 + SHARD_ID * STRIDE))}"
fi

ENV_ARGS=()
if [[ -n "${ENV_FILE:-}" && -f "$ENV_FILE" ]]; then
  ENV_ARGS=(--env-file "$ENV_FILE")
elif [[ -f ../.env ]]; then
  ENV_ARGS=(--env-file ../.env)
elif [[ -f .env ]]; then
  ENV_ARGS=(--env-file .env)
fi

echo "shard=${SHARD_ID} project=${COMPOSE_PROJECT_NAME} bind=${INGEST_BIND}"
echo "  ingest uncooked=${UNCOOKED_HOST_PORT} cooked=${COOKED_HOST_PORT}"
echo "  metrics classify=${CLASSIFY_HOST_PORT} s2s=${S2S_HEALTH_HOST_PORT} ls=${LS_API_HOST_PORT}"
echo "  prometheus=${PROMETHEUS_HOST_PORT} dlq-exporter=${DLQ_EXPORTER_HOST_PORT}"

docker compose \
  "${ENV_ARGS[@]}" \
  -f docker-compose.yml \
  -f docker-compose.shard.yml \
  "$@"
