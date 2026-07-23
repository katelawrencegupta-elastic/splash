"""Detect event types from Splunk metadata and message content (frosty-compatible)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum


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


def classify_event(
    *,
    sourcetype: str = "",
    source: str = "",
    message: str = "",
    splunk_index: str = "",
) -> ClassifiedEvent:
    """Determine event kind and target ingest pipeline for an event."""
    st = strip_splunk_prefix(sourcetype).lower()
    src = strip_splunk_prefix(source).lower()
    msg = message.strip()

    if st and ACCESS_SOURCETYPE_RE.search(st):
        kind = EventKind.ACCESS_LOG
        reason = f"sourcetype={st!r}"
    elif st and SYSLOG_SOURCETYPE_RE.search(st):
        kind = EventKind.SYSLOG
        reason = f"sourcetype={st!r}"
    elif src and ACCESS_SOURCE_RE.search(src):
        kind = EventKind.ACCESS_LOG
        reason = f"source={src!r}"
    elif src and SYSLOG_SOURCE_RE.search(src):
        kind = EventKind.SYSLOG
        reason = f"source={src!r}"
    elif ACCESS_MESSAGE_RE.match(msg):
        kind = EventKind.ACCESS_LOG
        reason = "message=access_log_pattern"
    elif SYSLOG_MESSAGE_RE.match(msg):
        kind = EventKind.SYSLOG
        reason = "message=syslog_pattern"
    else:
        kind = EventKind.GENERIC
        reason = "fallback=generic"

    dataset = f"{splunk_index}.{kind.value}" if splunk_index else kind.value
    return ClassifiedEvent(
        kind=kind,
        dataset=dataset,
        pipeline_name=parser_pipeline_name(kind),
        reason=reason,
    )


def data_stream_name(dataset: str, namespace: str = "default") -> str:
    """ECS data stream name: logs-{dataset}-{namespace}."""
    return f"logs-{dataset}-{namespace}"
