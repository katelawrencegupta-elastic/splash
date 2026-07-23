"""Unit tests for S2S signature / message / session."""

from __future__ import annotations

import pytest

from s2s.decoder import S2SSession
from s2s.handshake import SIGNATURE_SIZE
from s2s.kv import KvParseError, encode_string, pack_kv_payload, parse_kv_payload
from s2s.message import (
    S2SMessage,
    decode_message,
    encode_capabilities_reply,
    encode_message,
)
from s2s.normalize import to_logstash_event
from s2s.testdata import (
    SAMPLE_FIELDS,
    make_capabilities_message,
    make_event_frame,
    make_handshake,
)


def test_encode_string_null_terminated():
    assert encode_string("") == b"\x00\x00\x00\x01\x00"
    assert encode_string("hello") == b"\x00\x00\x00\x06hello\x00"


def test_kv_roundtrip():
    payload = pack_kv_payload({"name": "John"})
    parsed = parse_kv_payload(payload)
    assert parsed == {"name": "John"}


def test_kv_truncated_raises():
    payload = pack_kv_payload({"a": "b"})[:-2]
    with pytest.raises(KvParseError):
        parse_kv_payload(payload)


def test_message_roundtrip():
    original = S2SMessage(
        index="apache",
        host="web1",
        source="/var/log/nginx/access.log",
        sourcetype="access_combined",
        raw=SAMPLE_FIELDS["_raw"],
        time="1721577600",
        fields={"custom": "x"},
    )
    blob = encode_message(original)
    # body after size field
    import struct

    (size,) = struct.unpack(">I", blob[:4])
    assert size + 4 == len(blob)
    decoded = decode_message(blob[4:])
    assert decoded.index == original.index
    assert decoded.host == original.host
    assert decoded.source == original.source
    assert decoded.sourcetype == original.sourcetype
    assert decoded.raw == original.raw
    assert decoded.fields["custom"] == "x"


def test_normalize_maps_raw_and_index():
    event = to_logstash_event(SAMPLE_FIELDS, extra_tags=["t1"])
    assert event["message"] == SAMPLE_FIELDS["_raw"]
    assert event["splunk_index"] == "apache"
    assert event["sourcetype"] == "access_combined"
    assert event["_time"] == 1721577600.0
    assert event["tags"] == ["t1"]


def test_session_handshake_then_event():
    session = S2SSession()
    blob = make_handshake() + make_event_frame(SAMPLE_FIELDS)
    events = list(session.feed(blob))
    assert len(events) == 1
    assert session.stats.handshake_seen == 1
    assert session.stats.protocol_version == 3
    assert session.stats.frames_ok == 1
    assert events[0]["message"] == SAMPLE_FIELDS["_raw"]
    assert events[0]["splunk_index"] == "apache"
    assert "s2s_decoded" in events[0]["tags"]


def test_session_capabilities_reply():
    session = S2SSession()
    blob = (
        make_handshake()
        + make_capabilities_message("ack=0;compression=0")
        + make_event_frame(SAMPLE_FIELDS)
    )
    events = list(session.feed(blob))
    assert session.stats.capabilities_replied == 1
    replies = session.take_replies()
    assert len(replies) == 1
    assert b"__s2s_control_msg" in replies[0]
    assert b"cap_response=success" in replies[0]
    # Control reply must not include _done/_raw as map entries (maps=1).
    import struct

    (size,) = struct.unpack(">I", replies[0][:4])
    body = replies[0][4 : 4 + size]
    (maps,) = struct.unpack(">I", body[:4])
    assert maps == 1
    assert len(events) == 1




def test_capabilities_reply_encodes():
    reply = encode_capabilities_reply()
    assert b"cap_response=success" in reply


def test_session_partial_reads():
    session = S2SSession()
    blob = make_handshake() + make_event_frame(SAMPLE_FIELDS)
    mid = len(blob) // 2
    assert list(session.feed(blob[:mid])) == []
    events = list(session.feed(blob[mid:]))
    assert len(events) == 1
    assert events[0]["sourcetype"] == "access_combined"


def test_session_multi_frame():
    session = S2SSession()
    f1 = make_event_frame(SAMPLE_FIELDS)
    f2 = make_event_frame({**SAMPLE_FIELDS, "_raw": "second line", "index": "main"})
    events = list(session.feed(make_handshake() + f1 + f2))
    assert len(events) == 2
    assert events[1]["message"] == "second line"
    assert events[1]["splunk_index"] == "main"


def test_session_byte_at_a_time():
    session = S2SSession()
    blob = make_handshake() + make_event_frame(SAMPLE_FIELDS)
    assert len(blob) > SIGNATURE_SIZE
    events = []
    for b in blob:
        events.extend(session.feed(bytes([b])))
    assert len(events) == 1


def test_handshake_size():
    assert len(make_handshake()) == SIGNATURE_SIZE
