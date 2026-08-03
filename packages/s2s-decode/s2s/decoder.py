"""Incremental S2S session: signature + messages → events (+ capability replies)."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass, field, fields
from typing import Any, NamedTuple

from s2s.handshake import COOKED_BANNER_V2, COOKED_BANNER_V3, SIGNATURE_SIZE, parse_signature
from s2s.message import (
    DEFAULT_MAX_MESSAGE_SIZE,
    maybe_encode_capabilities_reply,
    message_to_flat_fields,
    try_read_message,
)
from s2s.normalize import to_logstash_event

logger = logging.getLogger(__name__)

# Default: 32 MiB — above one 16 MiB max frame, bounds slowloris / stuck clients.
DEFAULT_MAX_SESSION_BUFFER_BYTES = 32 * 1024 * 1024


class SessionBufferExceeded(ValueError):
    """Raised when the per-connection decode buffer exceeds its configured limit."""


class NeedMore(NamedTuple):
    """Incomplete frame; stop draining until more bytes arrive."""


class Skipped(NamedTuple):
    """Control message or framing error; keep draining."""


class DecodedEvent(NamedTuple):
    event: dict[str, Any]


@dataclass
class S2SStats:
    handshake_seen: int = 0
    frames_ok: int = 0  # messages decoded successfully
    frames_bad_magic: int = 0  # framing / parse errors
    frames_bad_kv: int = 0
    frames_oversized: int = 0
    events_emitted: int = 0
    bytes_consumed: int = 0
    capabilities_replied: int = 0
    protocol_version: int = 0

    def __iadd__(self, other: S2SStats) -> S2SStats:
        if not isinstance(other, S2SStats):
            return NotImplemented
        for f in fields(self):
            if f.name == "protocol_version":
                if other.protocol_version:
                    self.protocol_version = other.protocol_version
                continue
            setattr(self, f.name, getattr(self, f.name) + getattr(other, f.name))
        return self


@dataclass
class S2SSession:
    """Stateful decoder for one TCP connection."""

    max_frame_size: int = DEFAULT_MAX_MESSAGE_SIZE
    max_session_buffer_bytes: int = DEFAULT_MAX_SESSION_BUFFER_BYTES
    extra_tags: list[str] = field(default_factory=lambda: ["s2s_decoded", "splunk_tcp_39998"])
    stats: S2SStats = field(default_factory=S2SStats)
    _buf: bytearray = field(default_factory=bytearray, repr=False)
    _handshake_done: bool = False
    _replies: list[bytes] = field(default_factory=list, repr=False)

    def take_replies(self) -> list[bytes]:
        out = self._replies
        self._replies = []
        return out

    def feed(self, data: bytes) -> Iterator[dict[str, Any]]:
        if not data:
            return
        self._buf.extend(data)
        self.stats.bytes_consumed += len(data)
        if len(self._buf) > self.max_session_buffer_bytes:
            raise SessionBufferExceeded(
                f"session buffer {len(self._buf)} exceeds "
                f"max_session_buffer_bytes={self.max_session_buffer_bytes}"
            )
        yield from self._drain()

    def flush(self) -> Iterator[dict[str, Any]]:
        yield from self._drain()
        if self._buf:
            logger.warning("discarding %s trailing bytes at flush", len(self._buf))
            self._buf.clear()

    def _drain(self) -> Iterator[dict[str, Any]]:
        while True:
            if not self._handshake_done:
                if not self._try_consume_signature():
                    return
                continue

            result = self._try_consume_message()
            if isinstance(result, NeedMore):
                return
            if isinstance(result, Skipped):
                continue
            yield result.event

    def _try_consume_signature(self) -> bool:
        """Return True if signature was consumed; False if need more / failed."""
        if len(self._buf) < SIGNATURE_SIZE:
            # Wait if it looks like a partial banner prefix
            peek = memoryview(self._buf)
            if (
                COOKED_BANNER_V3.startswith(peek)
                or COOKED_BANNER_V2.startswith(peek)
                or peek[: min(8, len(peek))] == COOKED_BANNER_V3[: min(8, len(peek))]
                or peek[: min(8, len(peek))] == COOKED_BANNER_V2[: min(8, len(peek))]
            ):
                return False
            if len(self._buf) == 0:
                return False
            # No recognizable signature — proceed without (synthetic tests may omit it)
            self._handshake_done = True
            logger.info("no cooked signature; decoding messages from offset 0")
            return True

        parsed = parse_signature(memoryview(self._buf)[:SIGNATURE_SIZE])
        if parsed is None:
            # Maybe signature starts later with junk? Try find banner within first 64 bytes
            view = memoryview(self._buf)
            # memoryview has no .find; search in a bounded window only
            window = bytes(view[: min(len(view), 64 + SIGNATURE_SIZE)])
            idx = window.find(COOKED_BANNER_V3)
            if idx < 0:
                idx = window.find(COOKED_BANNER_V2)
            if 0 < idx <= 64 and len(self._buf) >= idx + SIGNATURE_SIZE:
                del self._buf[:idx]
                return True  # retry next loop
            # Give up on signature and try messages (may still fail)
            self._handshake_done = True
            logger.warning("unrecognized signature; attempting message decode")
            return True

        version, server, port = parsed
        del self._buf[:SIGNATURE_SIZE]
        self._handshake_done = True
        self.stats.handshake_seen += 1
        self.stats.protocol_version = version
        logger.info(
            "consumed cooked-mode v%s signature (%s bytes) server=%r port=%r",
            version,
            SIGNATURE_SIZE,
            server,
            port,
        )
        return True

    def _try_consume_message(self) -> NeedMore | Skipped | DecodedEvent:
        """Return NeedMore, Skipped, or DecodedEvent."""
        msg, consumed, err = try_read_message(
            memoryview(self._buf), max_size=self.max_frame_size
        )
        if consumed == 0 and err is None and msg is None:
            return NeedMore()
        if err is not None:
            if "oversized" in err:
                self.stats.frames_oversized += 1
            elif err.startswith("kv:"):
                self.stats.frames_bad_kv += 1
            else:
                self.stats.frames_bad_magic += 1
            skip = max(consumed, 1)
            logger.warning("message framing error: %s; skipping %s byte(s)", err, skip)
            del self._buf[:skip]
            return Skipped()
        assert msg is not None and consumed > 0
        del self._buf[:consumed]
        self.stats.frames_ok += 1

        if "__s2s_capabilities" in msg.fields:
            caps = msg.fields.get("__s2s_capabilities", "")
            reply = maybe_encode_capabilities_reply(caps)
            if reply is not None:
                self._replies.append(reply)
                self.stats.capabilities_replied += 1
                logger.info("replied to s2s capabilities: %s", caps)
            else:
                logger.info("ignoring s2s capabilities (no reply): %s", caps)
            if not msg.raw:
                return Skipped()

        if not msg.raw:
            # Control / flush with no payload
            return Skipped()

        flat = message_to_flat_fields(msg)
        event = to_logstash_event(flat, extra_tags=list(self.extra_tags))
        self.stats.events_emitted += 1
        return DecodedEvent(event)
