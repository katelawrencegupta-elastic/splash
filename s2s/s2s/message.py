"""S2S message encode/decode (size + maps + KV + _raw trailer).

Wire layout (after the 400-byte signature), matching go-s2s / eventgen:

  [u32 BE size]          # bytes after this field (includes maps count)
  [u32 BE maps]          # number of key/value pairs
  [KV] * maps            # null-terminated length-prefixed strings
  [u32 BE 0]             # padding
  [string "_raw"]        # trailer

Control/capability messages from real forwarders use maps=1 (no ``_done`` /
``_raw`` map entries). Data events include those map entries.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field

from s2s.kv import KvParseError, decode_key_value_at, decode_string_at, encode_key_value, encode_string

DEFAULT_MAX_MESSAGE_SIZE = 16 * 1024 * 1024  # 16 MiB

DEFAULT_CAP_RESPONSE = (
    "cap_response=success;cap_flush_key=false;idx_can_send_hb=false;"
    "idx_can_recv_token=false;request_certificate=false;v4=false;"
    "channel_limit=300;pl=0"
)


def build_cap_response(client_caps: str) -> str:
    """Build an indexer capability reply mirroring the forwarder advert."""
    caps = {
        part.split("=", 1)[0]: part.split("=", 1)[1]
        for part in client_caps.split(";")
        if "=" in part
    }
    pl = caps.get("pl", "0")
    v4 = caps.get("v4", "0")
    v4_flag = "true" if v4 in {"1", "true", "True"} else "false"
    return (
        "cap_response=success;"
        "cap_flush_key=false;"
        "idx_can_send_hb=false;"
        "idx_can_recv_token=false;"
        "request_certificate=false;"
        f"v4={v4_flag};"
        "channel_limit=300;"
        f"pl={pl}"
    )


@dataclass
class S2SMessage:
    index: str = ""
    host: str = ""
    source: str = ""
    sourcetype: str = ""
    raw: str = ""
    time: str = ""
    fields: dict[str, str] = field(default_factory=dict)

    def is_control(self) -> bool:
        return (not self.raw) and (
            "__s2s_capabilities" in self.fields or "__s2s_control_msg" in self.fields
        )


def _strip_prefix(value: str, prefix: str) -> str:
    return value[len(prefix) :] if value.startswith(prefix) else value


def _apply_kv(msg: S2SMessage, key: str, value: str) -> None:
    if key == "_MetaData:Index":
        msg.index = value
    elif key == "MetaData:Host":
        msg.host = _strip_prefix(value, "host::")
    elif key == "MetaData:Source":
        msg.source = _strip_prefix(value, "source::")
    elif key == "MetaData:Sourcetype":
        msg.sourcetype = _strip_prefix(value, "sourcetype::")
    elif key == "_time":
        msg.time = value
    elif key == "_done":
        return
    elif key == "_raw":
        msg.raw = value
    else:
        msg.fields[key] = value


def decode_message(body: bytes) -> S2SMessage:
    """Decode message body after the leading size field (starts with maps count)."""
    if len(body) < 4:
        raise KvParseError("message body too short for maps count")
    (maps,) = struct.unpack(">I", body[:4])
    offset = 4
    msg = S2SMessage()
    for _ in range(maps):
        key, value, offset = decode_key_value_at(body, offset)
        _apply_kv(msg, key, value)

    if offset + 4 > len(body):
        raise KvParseError("missing _raw padding")
    (padding,) = struct.unpack(">I", body[offset : offset + 4])
    offset += 4
    if padding != 0:
        raise KvParseError(f"unexpected padding value {padding}")

    trailer, offset = decode_string_at(body, offset)
    if trailer != "_raw":
        raise KvParseError(f"unexpected trailer {trailer!r}")
    return msg


def encode_message(msg: S2SMessage, *, include_done_raw: bool = True) -> bytes:
    """Encode a full message including the leading size field."""
    kvs: list[tuple[str, str]] = []
    if msg.index:
        kvs.append(("_MetaData:Index", msg.index))
    if msg.host:
        kvs.append(("MetaData:Host", "host::" + msg.host))
    if msg.source:
        kvs.append(("MetaData:Source", "source::" + msg.source))
    if msg.sourcetype:
        kvs.append(("MetaData:Sourcetype", "sourcetype::" + msg.sourcetype))
    for key, value in msg.fields.items():
        kvs.append((key, value))
    if msg.time:
        kvs.append(("_time", msg.time))
    if include_done_raw:
        kvs.append(("_done", "_done"))
        kvs.append(("_raw", msg.raw))

    parts = [encode_key_value(k, v) for k, v in kvs]
    kv_blob = b"".join(parts)
    maps = len(kvs)
    trailer = encode_string("_raw")
    body = struct.pack(">I", maps) + kv_blob + struct.pack(">I", 0) + trailer
    return struct.pack(">I", len(body)) + body


def encode_capabilities_reply(
    response: str = DEFAULT_CAP_RESPONSE,
) -> bytes:
    # Match forwarder control framing: maps=1, no _done/_raw map entries.
    return encode_message(
        S2SMessage(fields={"__s2s_control_msg": response}),
        include_done_raw=False,
    )


def maybe_encode_capabilities_reply(client_caps: str) -> bytes | None:
    return encode_capabilities_reply(build_cap_response(client_caps))


def try_read_message(
    buf: bytes, *, max_size: int = DEFAULT_MAX_MESSAGE_SIZE
) -> tuple[S2SMessage | None, int, str | None]:
    """Try to read one framed message from ``buf``.

    Returns ``(message, bytes_consumed, error)``.
    - ``(None, 0, None)`` → need more data
    - ``(None, n, err)`` → skip/resync consumed ``n`` bytes due to ``err``
    - ``(msg, n, None)`` → success
    """
    if len(buf) < 4:
        return None, 0, None
    (size,) = struct.unpack(">I", buf[:4])
    if size > max_size:
        return None, 1, f"oversized message size={size}"
    if size < 4:
        return None, 1, f"undersized message size={size}"
    total = 4 + size
    if len(buf) < total:
        return None, 0, None
    try:
        msg = decode_message(buf[4:total])
    except KvParseError as exc:
        return None, 1, str(exc)
    return msg, total, None


def message_to_flat_fields(msg: S2SMessage) -> dict[str, str]:
    """Flatten an S2SMessage into the dict expected by ``to_logstash_event``."""
    fields: dict[str, str] = {
        "host": msg.host,
        "source": msg.source,
        "sourcetype": msg.sourcetype,
        "index": msg.index,
        "_raw": msg.raw,
    }
    if msg.time:
        fields["_time"] = msg.time
    fields.update(msg.fields)
    return fields


DEFAULT_MAX_FRAME_SIZE = DEFAULT_MAX_MESSAGE_SIZE
