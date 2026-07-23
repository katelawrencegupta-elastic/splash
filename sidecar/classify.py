"""Detect event types from Splunk metadata and message content (frosty-compatible)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache


def strip_splunk_prefix(value: str) -> str:
    """Remove Splunk metadata prefixes like host::, source::, sourcetype::."""
    for prefix in ("host::", "source::", "sourcetype::"):
        if value.startswith(prefix):
            return value[len(prefix) :]
    return value


class EventKind(str, Enum):
    ACCESS_LOG = "access_log"
    SYSLOG = "syslog"
    GENERIC = "generic"


ACCESS_SOURCETYPE_RE = re.compile(
    r"(access|apache|nginx|iis|httpd|lb)",
    re.IGNORECASE,
)
SYSLOG_SOURCETYPE_RE = re.compile(r"syslog", re.IGNORECASE)
ACCESS_SOURCE_RE = re.compile(
    r"(/var/log/(nginx|apache2?|httpd)|access\.log)",
    re.IGNORECASE,
)
SYSLOG_SOURCE_RE = re.compile(r"/var/log/syslog", re.IGNORECASE)

# Combined / nginx-plus access log line
ACCESS_MESSAGE_RE = re.compile(
    r"^\S+\s+\S+\s+\S+\s+\[[^\]]+\]\s+\""
    r"(?:GET|POST|PUT|DELETE|HEAD|OPTIONS|PATCH|CONNECT|TRACE)\s",
    re.IGNORECASE,
)
# RFC3164 syslog with priority prefix
SYSLOG_MESSAGE_RE = re.compile(
    r"^<\d+>[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\S+",
)


@dataclass(frozen=True)
class ClassifiedEvent:
    kind: EventKind
    dataset: str
    pipeline_name: str
    reason: str


def parser_pipeline_name(kind: EventKind) -> str:
    return f"frosty-parse-{kind.value.replace('_', '-')}"


def _build(kind: EventKind, reason: str, splunk_index: str) -> ClassifiedEvent:
    dataset = f"{splunk_index}.{kind.value}" if splunk_index else kind.value
    return ClassifiedEvent(
        kind=kind,
        dataset=dataset,
        pipeline_name=parser_pipeline_name(kind),
        reason=reason,
    )


@lru_cache(maxsize=1024)
def _classify_from_metadata(
    sourcetype: str, source: str, splunk_index: str
) -> ClassifiedEvent | None:
    """Classify from sourcetype/source only. None → caller must use message."""
    st = strip_splunk_prefix(sourcetype).lower()
    src = strip_splunk_prefix(source).lower()

    if st and ACCESS_SOURCETYPE_RE.search(st):
        return _build(EventKind.ACCESS_LOG, f"sourcetype={st!r}", splunk_index)
    if st and SYSLOG_SOURCETYPE_RE.search(st):
        return _build(EventKind.SYSLOG, f"sourcetype={st!r}", splunk_index)
    if src and ACCESS_SOURCE_RE.search(src):
        return _build(EventKind.ACCESS_LOG, f"source={src!r}", splunk_index)
    if src and SYSLOG_SOURCE_RE.search(src):
        return _build(EventKind.SYSLOG, f"source={src!r}", splunk_index)
    return None


def classify_event(
    *,
    sourcetype: str = "",
    source: str = "",
    message: str = "",
    splunk_index: str = "",
) -> ClassifiedEvent:
    """Determine event kind and target ingest pipeline for an event."""
    index = (splunk_index or "").strip()
    cached = _classify_from_metadata(sourcetype or "", source or "", index)
    if cached is not None:
        return cached

    msg = message.strip()
    if ACCESS_MESSAGE_RE.match(msg):
        return _build(EventKind.ACCESS_LOG, "message=access_log_pattern", index)
    if SYSLOG_MESSAGE_RE.match(msg):
        return _build(EventKind.SYSLOG, "message=syslog_pattern", index)
    return _build(EventKind.GENERIC, "fallback=generic", index)


def data_stream_name(dataset: str, namespace: str = "default") -> str:
    """ECS data stream name: logs-{dataset}-{namespace}."""
    return f"logs-{dataset}-{namespace}"
