"""Shared generator counters."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class GenStats:
    sent_events: int = 0
    sent_bytes: int = 0
    errors: int = 0
    connects: int = 0
    started_at: float = field(default_factory=time.monotonic)

    def snapshot(self) -> dict[str, float | int]:
        elapsed = max(time.monotonic() - self.started_at, 1e-6)
        return {
            "sent_events": self.sent_events,
            "sent_bytes": self.sent_bytes,
            "errors": self.errors,
            "connects": self.connects,
            "eps": self.sent_events / elapsed,
            "mbps": (self.sent_bytes * 8) / elapsed / 1e6,
            "gbps_payload": self.sent_bytes / elapsed / 1e9,
            "elapsed_s": elapsed,
        }
