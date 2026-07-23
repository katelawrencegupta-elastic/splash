"""Legacy framing helpers.

Real Splunk cooked S2S does **not** use ``splk`` magic framing. Prefer
``s2s.message``. These helpers remain for any residual imports/tests.
"""

from __future__ import annotations

import struct

from s2s.message import DEFAULT_MAX_MESSAGE_SIZE as DEFAULT_MAX_FRAME_SIZE

SPLK_MAGIC = b"splk"
SPLK_MAGIC_INT = 0x73706C6B
HEADER_SIZE = 8


def pack_header(payload_len: int) -> bytes:
    if payload_len < 0:
        raise ValueError("payload_len must be >= 0")
    return SPLK_MAGIC + struct.pack(">I", payload_len)


def unpack_header(header: bytes) -> int:
    if len(header) < HEADER_SIZE:
        raise ValueError("header too short")
    magic, length = struct.unpack(">4sI", header[:HEADER_SIZE])
    if magic != SPLK_MAGIC:
        raise ValueError(f"bad magic {magic!r}, expected {SPLK_MAGIC!r}")
    return length
