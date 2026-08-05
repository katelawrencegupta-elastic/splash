#!/usr/bin/env bash
# Capture Splash compute-optimize baseline signals from a running stack.
# Usage:
#   S2S_METRICS=http://127.0.0.1:8081/metrics \
#   CLASSIFY_METRICS=http://127.0.0.1:8080/metrics \
#   ./scripts/compute-optimize-baseline.sh
set -euo pipefail

S2S_METRICS="${S2S_METRICS:-http://127.0.0.1:8081/metrics}"
CLASSIFY_METRICS="${CLASSIFY_METRICS:-http://127.0.0.1:8080/metrics}"

fetch() {
  local url="$1"
  curl -sf --max-time 5 "$url" || {
    echo "WARN: could not fetch $url" >&2
    return 1
  }
}

metric() {
  local body="$1" name="$2"
  printf '%s\n' "$body" | awk -v n="$name" '
    $1 == n || index($1, n "{") == 1 {
      print $NF
      exit
    }'
}

echo "=== Splash compute baseline $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
echo "s2s_metrics=$S2S_METRICS"
echo "classify_metrics=$CLASSIFY_METRICS"
echo

s2s="$(fetch "$S2S_METRICS" || true)"
cls="$(fetch "$CLASSIFY_METRICS" || true)"

if [[ -n "${s2s:-}" ]]; then
  echo "-- s2s-decode --"
  echo "upstream_queue=$(metric "$s2s" splash_s2s_upstream_queue)"
  echo "upstream_queue_capacity=$(metric "$s2s" splash_s2s_upstream_queue_capacity)"
  echo "events_emitted_total=$(metric "$s2s" splash_s2s_events_emitted_total)"
  echo "bytes_consumed_total=$(metric "$s2s" splash_s2s_bytes_consumed_total)"
  echo "avg_event_bytes=$(metric "$s2s" splash_s2s_avg_event_bytes)"
  echo "active_connections=$(metric "$s2s" splash_s2s_active_connections)"
  echo
else
  echo "-- s2s-decode: unavailable --"
  echo
fi

if [[ -n "${cls:-}" ]]; then
  echo "-- classify --"
  echo "pipelines_ready=$(metric "$cls" splash_pipelines_ready)"
  echo "classify_batch_events_total=$(metric "$cls" splash_classify_batch_events_total)"
  echo "classify_batch_requests_total=$(metric "$cls" splash_classify_batch_requests_total)"
  echo "ensure_batch_streams_total=$(metric "$cls" splash_ensure_batch_streams_total)"
  echo
else
  echo "-- classify: unavailable --"
  echo
fi

cat <<'EOF'
Interpret:
  - upstream_queue near capacity → Logstash/ES behind (Phase 2 / shard).
  - classify_batch_events rising on hot soak → miss path tax (Phase 1).
  - Prefer Prometheus: splash:ingest_gbps:5m, splash:miss_fraction:1m,
    splash:hit_fraction:1m (see deploy/alerts/splash-recording.yaml).
  - Planning floor remains 0.008 GB/s/stack until Phase 2 vertical probe wins.
EOF
