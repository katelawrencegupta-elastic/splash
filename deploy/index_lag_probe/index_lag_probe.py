#!/usr/bin/env python3
"""Compare Splash offered events vs Elasticsearch indexed count; export lag.

Exports Prometheus gauge splash_index_lag_seconds on :9103/metrics.

Env:
  ELASTIC_HOST          required
  ELASTIC_API_KEY       required (id:secret or raw base64)
  DATA_STREAM_NAMESPACE default "default" — counts logs-*-{namespace}
  S2S_HEALTH_URLS       comma-separated http://host:8081/health (default s2s-decode:8081)
  INTERVAL_SECONDS      poll interval (default 15)
  LISTEN_HOST / LISTEN_PORT  default 0.0.0.0:9103
"""

from __future__ import annotations

import base64
import json
import os
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


ELASTIC_HOST = os.environ.get("ELASTIC_HOST", "").strip().rstrip("/")
ELASTIC_API_KEY = os.environ.get("ELASTIC_API_KEY", "").strip()
NAMESPACE = os.environ.get("DATA_STREAM_NAMESPACE", "default").strip() or "default"
S2S_HEALTH_URLS = [
    u.strip()
    for u in os.environ.get("S2S_HEALTH_URLS", "http://s2s-decode:8081/health").split(",")
    if u.strip()
]
INTERVAL = max(5, int(os.environ.get("INTERVAL_SECONDS", "15")))
LISTEN_HOST = os.environ.get("LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "9103"))

_lock = threading.Lock()
_lag_seconds = 0.0
_offered_eps = 0.0
_indexed_eps = 0.0
_last_error = ""


def _api_key_header(raw: str) -> str:
    key = raw.strip()
    if ":" in key:
        key = base64.b64encode(key.encode("utf-8")).decode("ascii")
    return f"ApiKey {key}"


def _http_json(url: str, headers: dict[str, str] | None = None) -> dict:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _sum_events_emitted() -> int:
    total = 0
    for url in S2S_HEALTH_URLS:
        data = _http_json(url)
        total += int(data.get("stats", {}).get("events_emitted", 0))
    return total


def _es_count() -> int:
    if not ELASTIC_HOST or not ELASTIC_API_KEY:
        raise RuntimeError("ELASTIC_HOST and ELASTIC_API_KEY required")
    index = f"logs-*-{NAMESPACE}"
    url = f"{ELASTIC_HOST}/{index}/_count"
    headers = {
        "Authorization": _api_key_header(ELASTIC_API_KEY),
        "Content-Type": "application/json",
    }
    data = _http_json(url, headers=headers)
    return int(data.get("count", 0))


def _poll_loop() -> None:
    global _lag_seconds, _offered_eps, _indexed_eps, _last_error
    prev_offered: int | None = None
    prev_indexed: int | None = None
    prev_ts: float | None = None
    while True:
        try:
            offered = _sum_events_emitted()
            indexed = _es_count()
            now = time.time()
            with _lock:
                _last_error = ""
                if prev_offered is not None and prev_indexed is not None and prev_ts:
                    dt = max(now - prev_ts, 1e-6)
                    _offered_eps = (offered - prev_offered) / dt
                    _indexed_eps = (indexed - prev_indexed) / dt
                    # Positive lag when offered runs ahead of indexed.
                    gap = max(0, offered - indexed)
                    # Convert event gap to seconds using recent indexed rate (or offered).
                    rate = _indexed_eps if _indexed_eps > 1.0 else max(_offered_eps, 1.0)
                    _lag_seconds = gap / rate
                prev_offered = offered
                prev_indexed = indexed
                prev_ts = now
        except Exception as exc:  # noqa: BLE001 — keep probe alive
            with _lock:
                _last_error = f"{type(exc).__name__}: {exc}"
        time.sleep(INTERVAL)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path not in ("/metrics", "/"):
            self.send_response(404)
            self.end_headers()
            return
        with _lock:
            lag = _lag_seconds
            offered = _offered_eps
            indexed = _indexed_eps
            err = _last_error
        body = "\n".join(
            [
                "# HELP splash_index_lag_seconds Estimated seconds of index lag",
                "# TYPE splash_index_lag_seconds gauge",
                f"splash_index_lag_seconds {lag:.3f}",
                "# HELP splash_index_offered_eps Recent offered events/s from s2s",
                "# TYPE splash_index_offered_eps gauge",
                f"splash_index_offered_eps {offered:.3f}",
                "# HELP splash_index_indexed_eps Recent ES _count delta events/s",
                "# TYPE splash_index_indexed_eps gauge",
                f"splash_index_indexed_eps {indexed:.3f}",
                f"# error {err}" if err else "# error none",
                "",
            ]
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


def main() -> None:
    if not ELASTIC_HOST or not ELASTIC_API_KEY:
        raise SystemExit("ELASTIC_HOST and ELASTIC_API_KEY are required")
    threading.Thread(target=_poll_loop, name="index-lag-poll", daemon=True).start()
    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    print(
        f"index_lag_probe listening on {LISTEN_HOST}:{LISTEN_PORT} "
        f"ns={NAMESPACE} s2s={S2S_HEALTH_URLS}",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
