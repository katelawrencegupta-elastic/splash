"""Shared golden fixtures: Python and Ruby must pass the same corpus.

Protocol / framing changes require updating files under ``splash/testdata/s2s/``
and re-running both:

  cd splash/s2s && pytest tests/test_golden.py
  ruby splash/logstash/plugins/logstash-input-s2s/test_decoder.rb

Production cooked path is Python ``s2s-decode``. The Ruby Logstash input is a
port kept in sync via these goldens (not optional for protocol fixes).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from s2s.decoder import S2SSession
from s2s.message import try_read_message

# splash/testdata/s2s (repo-relative from s2s package tests)
GOLDEN_ROOT = Path(__file__).resolve().parents[2] / "testdata" / "s2s"


def _load_manifest() -> list[dict]:
    manifest = json.loads((GOLDEN_ROOT / "manifest.json").read_text(encoding="utf-8"))
    return list(manifest["cases"])


@pytest.mark.parametrize("case", _load_manifest(), ids=lambda c: c["id"])
def test_golden_case(case: dict) -> None:
    blob = (GOLDEN_ROOT / case["bin"]).read_bytes()
    expect = case["expect"]
    session = S2SSession()
    events = list(session.feed(blob))
    assert len(events) == expect["events"]
    if "message" in expect:
        assert events[0]["message"] == expect["message"]
    if "splunk_index" in expect:
        assert events[0]["splunk_index"] == expect["splunk_index"]
    if "sourcetype" in expect:
        assert events[0]["sourcetype"] == expect["sourcetype"]

    stats = session.stats
    for key in (
        "frames_ok",
        "handshake_seen",
        "frames_bad_magic",
        "frames_bad_kv",
        "frames_oversized",
        "capabilities_replied",
    ):
        if key in expect:
            assert getattr(stats, key) == expect[key], key
    for key, attr in (
        ("frames_ok_min", "frames_ok"),
        ("frames_bad_magic_min", "frames_bad_magic"),
        ("frames_bad_kv_min", "frames_bad_kv"),
        ("frames_oversized_min", "frames_oversized"),
    ):
        if key in expect:
            assert getattr(stats, attr) >= expect[key], attr


def test_try_read_message_accepts_memoryview() -> None:
    from s2s.testdata import SAMPLE_FIELDS, make_event_frame

    raw = make_event_frame(SAMPLE_FIELDS)
    buf = bytearray(raw)
    msg, consumed, err = try_read_message(memoryview(buf))
    assert err is None
    assert msg is not None
    assert consumed == len(raw)
    assert msg.raw == SAMPLE_FIELDS["_raw"]


def test_bad_kv_increments_frames_bad_kv() -> None:
    case = next(c for c in _load_manifest() if c["id"] == "bad_kv")
    blob = (GOLDEN_ROOT / case["bin"]).read_bytes()
    session = S2SSession()
    list(session.feed(blob))
    assert session.stats.frames_bad_kv >= 1
