"""Normalize S2S fields to Logstash / classify schema."""

from __future__ import annotations

from typing import Any


KNOWN_KEYS = frozenset(
    {
        "_raw",
        "host",
        "source",
        "sourcetype",
        "index",
        "_time",
        "_orphan",
        "_done",
        "_MetaData:Index",
        "MetaData:Host",
        "MetaData:Source",
        "MetaData:Sourcetype",
        "__s2s_capabilities",
        "__s2s_control_msg",
    }
)


def to_logstash_event(
    fields: dict[str, str],
    *,
    extra_tags: list[str] | None = None,
) -> dict[str, Any]:
    """Map S2S fields to the splash Logstash/classify event contract."""
    message = fields.get("_raw") or fields.get("message") or ""
    event: dict[str, Any] = {
        "host": fields.get("host", ""),
        "source": fields.get("source", ""),
        "sourcetype": fields.get("sourcetype", ""),
        "splunk_index": fields.get("index", ""),
        "message": message,
        "tags": list(extra_tags or []),
    }
    if "_time" in fields and fields["_time"] != "":
        try:
            event["_time"] = float(fields["_time"])
        except ValueError:
            event["_time"] = fields["_time"]

    extras = {k: v for k, v in fields.items() if k not in KNOWN_KEYS}
    if extras:
        event["s2s"] = {"fields": extras}
    if "_orphan" in fields:
        event.setdefault("s2s", {}).setdefault("fields", {})["_orphan"] = fields["_orphan"]
    return event
