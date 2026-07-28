"""Shared ~1.5 KB event payloads for hot and cold classify paths."""

from __future__ import annotations

import json
import time
from typing import Any


ACCESS_LINE = (
    '1.2.3.4 - - [21/Jul/2026:12:00:00 +0000] '
    '"GET /api/v1/items?id=42 HTTP/1.1" 200 1234 '
    '"https://example.com/" "Mozilla/5.0 loadtest"'
)


def _pad(body: str, target_bytes: int) -> str:
    raw = body.encode("utf-8")
    if len(raw) >= target_bytes:
        return body
    pad_len = target_bytes - len(raw)
    # Keep it log-like; avoid newlines so uncooked stays one event per line.
    return body + (" " + ("x" * max(pad_len - 1, 0)))


def hot_fields(*, seq: int, event_bytes: int, namespace_tag: str = "loadtest") -> dict[str, str]:
    """Metadata that hits access_combined rules (local classify)."""
    raw = _pad(f"{ACCESS_LINE} seq={seq} ns={namespace_tag}", event_bytes)
    return {
        "host": f"lt-host-{(seq % 16) + 1}",
        "source": "/var/log/nginx/access.log",
        "sourcetype": "access_combined",
        "index": "apache",
        "_time": f"{time.time():.3f}",
        "_raw": raw,
    }


def cold_fields(*, seq: int, event_bytes: int, namespace_tag: str = "loadtest") -> dict[str, str]:
    """Empty sourcetype/source → message-path /classify/batch."""
    # Access-like message so sidecar message regex can still classify kind.
    raw = _pad(f"{ACCESS_LINE} seq={seq} ns={namespace_tag} cold=1", event_bytes)
    return {
        "host": f"lt-host-{(seq % 16) + 1}",
        "source": "",
        "sourcetype": "",
        "index": "main",
        "_time": f"{time.time():.3f}",
        "_raw": raw,
    }


def fields_for(*, hot: bool, seq: int, event_bytes: int) -> dict[str, str]:
    if hot:
        return hot_fields(seq=seq, event_bytes=event_bytes)
    return cold_fields(seq=seq, event_bytes=event_bytes)


def uncooked_line(*, hot: bool, seq: int, event_bytes: int) -> bytes:
    """JSON line for Logstash json filter on :39997 (when message looks like JSON)."""
    f = fields_for(hot=hot, seq=seq, event_bytes=event_bytes)
    doc: dict[str, Any] = {
        "host": f["host"],
        "source": f["source"],
        "sourcetype": f["sourcetype"],
        "splunk_index": f["index"],
        "message": f["_raw"],
        "_time": float(f["_time"]),
        "tags": ["loadtest_uncooked"],
    }
    return (json.dumps(doc, separators=(",", ":")) + "\n").encode("utf-8")
