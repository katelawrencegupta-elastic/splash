"""Metadata classify rules cover expanded access/syslog patterns (compute Phase 1)."""

from __future__ import annotations

from classify import classify_event


def test_expanded_access_sourcetypes_hit_metadata():
    for st in ("haproxy:http", "traefik", "caddy:access", "squid", "aws:elb"):
        ev = classify_event(sourcetype=st, source="", message="", splunk_index="main")
        assert ev.kind.value == "access_log", st
        assert "sourcetype=" in ev.reason


def test_expanded_syslog_sourcetypes_hit_metadata():
    for st in ("linux_messages_syslog", "cisco:asa", "syslog"):
        ev = classify_event(sourcetype=st, source="", message="x", splunk_index="")
        assert ev.kind.value == "syslog", st


def test_expanded_source_paths_hit_metadata():
    ev = classify_event(
        sourcetype="",
        source="/var/log/haproxy/access.log",
        message="",
        splunk_index="web",
    )
    assert ev.kind.value == "access_log"
    ev2 = classify_event(
        sourcetype="",
        source="/var/log/secure",
        message="",
        splunk_index="",
    )
    assert ev2.kind.value == "syslog"


def test_empty_metadata_still_uses_message_or_generic():
    ev = classify_event(sourcetype="", source="", message="not a known pattern", splunk_index="")
    assert ev.kind.value == "generic"
