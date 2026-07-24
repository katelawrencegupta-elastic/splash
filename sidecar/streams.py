"""Ensure ECS data streams exist in Elasticsearch (idempotent)."""

from __future__ import annotations

import base64
import logging
import os
import threading
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# Short timeouts so a slow/unreachable ES cannot stall a whole classify batch.
_ES_TIMEOUT_S = float(os.environ.get("ELASTIC_HTTP_TIMEOUT_S", "2.0"))


def _api_key_header(api_key: str) -> str:
    """Encode id:api_key for Authorization: ApiKey <base64>."""
    raw = api_key.strip()
    # Already base64 (no colon) — use as-is; id:secret form must be encoded.
    if ":" in raw:
        return base64.b64encode(raw.encode("utf-8")).decode("ascii")
    return raw

TEMPLATE_NAME = "splash-logs"
TEMPLATE_PATTERNS = ["logs-*-*"]

# Composable index template with data_stream mode + frosty-aligned mappings.
INDEX_TEMPLATE: dict[str, Any] = {
    "index_patterns": TEMPLATE_PATTERNS,
    "data_stream": {},
    "priority": 500,
    "template": {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "message": {"type": "text"},
                "host": {"type": "keyword"},
                "source": {"type": "keyword"},
                "sourcetype": {"type": "keyword"},
                "data_stream": {
                    "properties": {
                        "type": {"type": "keyword"},
                        "dataset": {"type": "keyword"},
                        "namespace": {"type": "keyword"},
                    }
                },
                "event": {
                    "properties": {
                        "kind": {"type": "keyword"},
                        "dataset": {"type": "keyword"},
                        "category": {"type": "keyword"},
                        "original": {"type": "text"},
                    }
                },
                "splunk": {
                    "properties": {
                        "pipeline": {"type": "keyword"},
                        "classify_reason": {"type": "keyword"},
                        "index": {"type": "keyword"},
                    }
                },
            }
        },
    },
}


class DataStreamManager:
    """Creates index template + data streams; caches names already ensured.

    HTTP runs outside the cache lock. Concurrent ensures for the same name
    coalesce on an in-flight Event so only one PUT is issued.
    """

    def __init__(self, elastic_host: str, api_key: str) -> None:
        self._host = elastic_host.rstrip("/")
        self._headers = {
            "Authorization": f"ApiKey {_api_key_header(api_key)}",
            "Content-Type": "application/json",
        }
        timeout = httpx.Timeout(_ES_TIMEOUT_S, connect=_ES_TIMEOUT_S)
        self._client = httpx.Client(timeout=timeout, headers=self._headers)
        self._ensured: set[str] = set()
        self._lock = threading.Lock()
        self._template_ready = False
        self._template_event: threading.Event | None = None
        self._inflight: dict[str, threading.Event] = {}

    def close(self) -> None:
        self._client.close()

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict | None = None,
    ) -> tuple[int, Any]:
        url = f"{self._host}{path}"
        resp = self._client.request(method, url, json=json_body)
        try:
            body: Any = resp.json()
        except Exception:
            body = resp.text
        return resp.status_code, body

    def ensure_template(self) -> None:
        if self._template_ready:
            return

        wait_event: threading.Event | None = None
        do_work = False
        with self._lock:
            if self._template_ready:
                return
            if self._template_event is not None:
                wait_event = self._template_event
            else:
                wait_event = threading.Event()
                self._template_event = wait_event
                do_work = True

        if not do_work:
            assert wait_event is not None
            if not wait_event.wait(timeout=_ES_TIMEOUT_S + 1.0):
                raise RuntimeError("index template ensure timed out waiting")
            if not self._template_ready:
                raise RuntimeError("index template ensure failed in another thread")
            return

        try:
            status, body = self._request(
                "PUT",
                f"/_index_template/{TEMPLATE_NAME}",
                json_body=INDEX_TEMPLATE,
            )
            if status not in (200, 201):
                raise RuntimeError(
                    f"index template put failed status={status} body={body}"
                )
            with self._lock:
                self._template_ready = True
            logger.info("Ensured index template %s", TEMPLATE_NAME)
        finally:
            with self._lock:
                self._template_event = None
            wait_event.set()

    def ensure_data_stream(self, name: str) -> None:
        """Idempotently create data stream ``name`` (e.g. logs-access_log-default).

        PUT-only: ``resource_already_exists_exception`` is treated as success, so
        a prior GET is unnecessary. Lock is held only around cache / in-flight
        bookkeeping — never across HTTP.
        """
        if name in self._ensured:
            return

        wait_event: threading.Event | None = None
        do_work = False
        with self._lock:
            if name in self._ensured:
                return
            existing = self._inflight.get(name)
            if existing is not None:
                wait_event = existing
            else:
                wait_event = threading.Event()
                self._inflight[name] = wait_event
                do_work = True

        if not do_work:
            assert wait_event is not None
            if not wait_event.wait(timeout=_ES_TIMEOUT_S + 1.0):
                raise RuntimeError(f"data stream ensure timed out waiting name={name}")
            if name not in self._ensured:
                raise RuntimeError(f"data stream ensure failed in another thread name={name}")
            return

        try:
            self.ensure_template()
            status, body = self._request("PUT", f"/_data_stream/{name}")
            if status in (200, 201):
                with self._lock:
                    self._ensured.add(name)
                logger.info("Created data stream %s", name)
                return
            if status == 400 and isinstance(body, dict):
                err_type = body.get("error", {}).get("type", "")
                if err_type == "resource_already_exists_exception":
                    with self._lock:
                        self._ensured.add(name)
                    logger.debug("Data stream already exists: %s", name)
                    return
            raise RuntimeError(
                f"data stream create failed name={name} status={status} body={body}"
            )
        finally:
            with self._lock:
                self._inflight.pop(name, None)
            wait_event.set()
