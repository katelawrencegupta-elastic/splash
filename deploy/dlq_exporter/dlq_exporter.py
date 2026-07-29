#!/usr/bin/env python3
"""Tiny Prometheus exporter for Logstash dead_letter_queue depth.

Mount Logstash data dir and scrape :9102/metrics.

  DLQ_PATH=/usr/share/logstash/data/dead_letter_queue
  LISTEN_PORT=9102
"""

from __future__ import annotations

import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


DLQ_PATH = Path(os.environ.get("DLQ_PATH", "/usr/share/logstash/data/dead_letter_queue"))
LISTEN_HOST = os.environ.get("LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "9102"))


def _dlq_stats() -> tuple[int, int]:
    """Return (file_count, total_bytes) under DLQ_PATH."""
    if not DLQ_PATH.exists():
        return 0, 0
    files = 0
    total = 0
    for root, _dirs, names in os.walk(DLQ_PATH):
        for name in names:
            path = Path(root) / name
            try:
                total += path.stat().st_size
                files += 1
            except OSError:
                continue
    return files, total


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path not in ("/metrics", "/"):
            self.send_response(404)
            self.end_headers()
            return
        files, nbytes = _dlq_stats()
        body = "\n".join(
            [
                "# HELP splash_dlq_files Files under Logstash dead_letter_queue",
                "# TYPE splash_dlq_files gauge",
                f"splash_dlq_files {files}",
                "# HELP splash_dlq_bytes_total Bytes under Logstash dead_letter_queue",
                "# TYPE splash_dlq_bytes_total gauge",
                f"splash_dlq_bytes_total {nbytes}",
                f"# scraped_at {int(time.time())}",
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
    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    print(f"dlq_exporter listening on {LISTEN_HOST}:{LISTEN_PORT} path={DLQ_PATH}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
