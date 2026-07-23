"""Null-terminated length-prefixed UTF-8 strings (S2S wire format)."""

from __future__ import annotations

import struct
from typing import Iterator


class KvParseError(ValueError):
    """Raised when length-prefixed strings cannot be parsed."""


def encode_string(value: str) -> bytes:
    """``[u32 BE length including NUL][utf-8 bytes][NUL]``."""
    data = value.encode("utf-8")
    return struct.pack(">I", len(data) + 1) + data + b"\x00"


def decode_string_at(buf: bytes, offset: int) -> tuple[str, int]:
    """Decode one string at ``offset``; return ``(value, next_offset)``."""
    if offset + 4 > len(buf):
        raise KvParseError(f"truncated length prefix at offset {offset}")
    (length,) = struct.unpack(">I", buf[offset : offset + 4])
    offset += 4
    if length < 1:
        raise KvParseError(f"invalid string length {length} at offset {offset - 4}")
    end = offset + length
    if end > len(buf):
        raise KvParseError(f"string length {length} exceeds buffer at offset {offset}")
    if buf[end - 1] != 0:
        raise KvParseError(f"missing NUL terminator at offset {end - 1}")
    raw = buf[offset : end - 1]
    try:
        return raw.decode("utf-8"), end
    except UnicodeDecodeError as exc:
        raise KvParseError(f"invalid UTF-8 at offset {offset}") from exc


def encode_key_value(key: str, value: str) -> bytes:
    return encode_string(key) + encode_string(value)


def decode_key_value_at(buf: bytes, offset: int) -> tuple[str, str, int]:
    key, offset = decode_string_at(buf, offset)
    value, offset = decode_string_at(buf, offset)
    return key, value, offset


def iter_length_prefixed_strings(payload: bytes) -> Iterator[str]:
    """Yield strings from a contiguous payload (no message framing)."""
    offset = 0
    n = len(payload)
    while offset < n:
        value, offset = decode_string_at(payload, offset)
        yield value


def parse_kv_payload(payload: bytes) -> dict[str, str]:
    """Parse alternating key/value strings into a dict (odd trailing → ``_orphan``)."""
    tokens = list(iter_length_prefixed_strings(payload))
    fields: dict[str, str] = {}
    i = 0
    while i + 1 < len(tokens):
        fields[tokens[i]] = tokens[i + 1]
        i += 2
    if i < len(tokens):
        fields["_orphan"] = tokens[i]
    return fields


def pack_length_prefixed(value: str) -> bytes:
    return encode_string(value)


def pack_kv_payload(fields: dict[str, str]) -> bytes:
    parts: list[bytes] = []
    for key, value in fields.items():
        parts.append(encode_key_value(key, value))
    return b"".join(parts)
