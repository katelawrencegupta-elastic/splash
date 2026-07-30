"""Build synthetic S2S cooked fixtures for tests."""

from __future__ import annotations

from s2s.handshake import pack_signature
from s2s.message import S2SMessage, encode_message


def make_event_frame(fields: dict[str, str]) -> bytes:
    """Encode a data message from flat test fields (host/source/.../_raw)."""
    msg = S2SMessage(
        index=fields.get("index", ""),
        host=fields.get("host", ""),
        source=fields.get("source", ""),
        sourcetype=fields.get("sourcetype", ""),
        raw=fields.get("_raw", ""),
        time=fields.get("_time", ""),
        fields={
            k: v
            for k, v in fields.items()
            if k
            not in {
                "index",
                "host",
                "source",
                "sourcetype",
                "_raw",
                "_time",
            }
        },
    )
    return encode_message(msg)


def make_handshake(version: int = 3) -> bytes:
    return pack_signature(version=version)


def make_capabilities_message(
    caps: str = "ack=0;compression=0",
) -> bytes:
    return encode_message(
        S2SMessage(fields={"__s2s_capabilities": caps}),
        include_done_raw=False,
    )


SAMPLE_FIELDS = {
    "host": "web1",
    "source": "/var/log/nginx/access.log",
    "sourcetype": "access_combined",
    "index": "apache",
    "_time": "1721577600.0",
    "_raw": '1.2.3.4 - - [21/Jul/2026:12:00:00 +0000] "GET / HTTP/1.1" 200 1234 "-" "curl"',
}
