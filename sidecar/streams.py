"""Ensure ECS data streams + ingest pipelines exist in Elasticsearch (idempotent, async)."""

from __future__ import annotations

import asyncio
import base64
import logging
import os
from typing import Any

import httpx

from classify import EventKind, parser_pipeline_name

logger = logging.getLogger(__name__)

# Short timeouts so a slow/unreachable ES cannot stall a whole classify batch.
_ES_TIMEOUT_S = float(os.environ.get("ELASTIC_HTTP_TIMEOUT_S", "2.0"))

# require = pipelines must already exist (production). stub = create empty stubs (POC).
FROSTY_PIPELINE_MODE = os.environ.get("FROSTY_PIPELINE_MODE", "require").strip().lower()
if FROSTY_PIPELINE_MODE not in {"require", "stub"}:
    raise ValueError(
        f"FROSTY_PIPELINE_MODE must be 'require' or 'stub', got {FROSTY_PIPELINE_MODE!r}"
    )

# Empty stub when frosty parsers are not yet installed (stub mode only).
# Existing pipelines are left untouched (GET-then-PUT-if-missing).
_STUB_INGEST_PIPELINE: dict[str, Any] = {
    "description": (
        "Splash-managed stub (no processors). Replace with frosty parsers "
        "in the cluster when available; Splash will not overwrite an existing pipeline."
    ),
    "processors": [],
}


def required_ingest_pipelines() -> list[str]:
    """Canonical frosty-parse-* names derived from classify rules."""
    return [parser_pipeline_name(kind) for kind in EventKind]


def _coalesce_wait_s(http_steps: int) -> float:
    """How long a waiter may block for ``http_steps`` sequential ES calls + slack.

    Stream ensure can run template PUT then stream PUT (2 steps). Waiters must
    outlive that worst case or they spuriously fall back while the worker succeeds.
    """
    return http_steps * _ES_TIMEOUT_S + 1.0


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
    coalesce on an in-flight asyncio.Event so only one PUT is issued.
    """

    def __init__(
        self,
        elastic_host: str,
        api_key: str,
        *,
        frosty_pipeline_mode: str | None = None,
    ) -> None:
        self._host = elastic_host.rstrip("/")
        self._headers = {
            "Authorization": f"ApiKey {_api_key_header(api_key)}",
            "Content-Type": "application/json",
        }
        timeout = httpx.Timeout(_ES_TIMEOUT_S, connect=_ES_TIMEOUT_S)
        self._client = httpx.AsyncClient(timeout=timeout, headers=self._headers)
        self._ensured: set[str] = set()
        self._lock = asyncio.Lock()
        self._template_ready = False
        self._template_event: asyncio.Event | None = None
        self._inflight: dict[str, asyncio.Event] = {}
        mode = (frosty_pipeline_mode or FROSTY_PIPELINE_MODE).strip().lower()
        if mode not in {"require", "stub"}:
            raise ValueError(f"frosty_pipeline_mode must be require|stub, got {mode!r}")
        self._frosty_pipeline_mode = mode

    async def close(self) -> None:
        await self._client.aclose()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict | None = None,
    ) -> tuple[int, Any]:
        url = f"{self._host}{path}"
        resp = await self._client.request(method, url, json=json_body)
        try:
            body: Any = resp.json()
        except Exception:
            body = resp.text
        return resp.status_code, body

    async def ensure_template(self) -> None:
        if self._template_ready:
            return

        wait_event: asyncio.Event | None = None
        do_work = False
        async with self._lock:
            if self._template_ready:
                return
            if self._template_event is not None:
                wait_event = self._template_event
            else:
                wait_event = asyncio.Event()
                self._template_event = wait_event
                do_work = True

        if not do_work:
            assert wait_event is not None
            try:
                await asyncio.wait_for(wait_event.wait(), timeout=_coalesce_wait_s(1))
            except asyncio.TimeoutError as exc:
                raise RuntimeError("index template ensure timed out waiting") from exc
            if not self._template_ready:
                raise RuntimeError("index template ensure failed in another task")
            return

        try:
            status, body = await self._request(
                "PUT",
                f"/_index_template/{TEMPLATE_NAME}",
                json_body=INDEX_TEMPLATE,
            )
            if status not in (200, 201):
                raise RuntimeError(
                    f"index template put failed status={status} body={body}"
                )
            async with self._lock:
                self._template_ready = True
            logger.info("Ensured index template %s", TEMPLATE_NAME)
        finally:
            async with self._lock:
                self._template_event = None
            wait_event.set()

    async def ensure_data_stream(self, name: str) -> None:
        """Idempotently create data stream ``name`` (e.g. logs-access_log-default).

        PUT-only: ``resource_already_exists_exception`` is treated as success, so
        a prior GET is unnecessary. Lock is held only around cache / in-flight
        bookkeeping — never across HTTP.
        """
        if name in self._ensured:
            return

        wait_event: asyncio.Event | None = None
        do_work = False
        async with self._lock:
            if name in self._ensured:
                return
            existing = self._inflight.get(name)
            if existing is not None:
                wait_event = existing
            else:
                wait_event = asyncio.Event()
                self._inflight[name] = wait_event
                do_work = True

        if not do_work:
            assert wait_event is not None
            try:
                await asyncio.wait_for(wait_event.wait(), timeout=_coalesce_wait_s(2))
            except asyncio.TimeoutError as exc:
                raise RuntimeError(
                    f"data stream ensure timed out waiting name={name}"
                ) from exc
            if name not in self._ensured:
                raise RuntimeError(
                    f"data stream ensure failed in another task name={name}"
                )
            return

        try:
            await self.ensure_template()
            status, body = await self._request("PUT", f"/_data_stream/{name}")
            if status in (200, 201):
                async with self._lock:
                    self._ensured.add(name)
                logger.info("Created data stream %s", name)
                return
            if status == 400 and isinstance(body, dict):
                err_type = body.get("error", {}).get("type", "")
                if err_type == "resource_already_exists_exception":
                    async with self._lock:
                        self._ensured.add(name)
                    logger.debug("Data stream already exists: %s", name)
                    return
            raise RuntimeError(
                f"data stream create failed name={name} status={status} body={body}"
            )
        finally:
            async with self._lock:
                self._inflight.pop(name, None)
            wait_event.set()

    async def ensure_ingest_pipeline(self, name: str) -> None:
        """Ensure ingest pipeline ``name`` exists.

        - require mode: GET must succeed; missing pipelines raise (no stubs).
        - stub mode: create an empty stub if missing; never overwrite existing.
        """
        if not name:
            raise ValueError("ingest pipeline name is required")

        status, body = await self._request("GET", f"/_ingest/pipeline/{name}")
        if status == 200:
            logger.debug("Ingest pipeline already present: %s", name)
            return
        missing = status == 404
        if (
            not missing
            and status == 400
            and isinstance(body, dict)
            and body.get("error", {}).get("type") == "resource_not_found_exception"
        ):
            missing = True
        if not missing:
            raise RuntimeError(
                f"ingest pipeline get failed name={name} status={status} body={body}"
            )

        if self._frosty_pipeline_mode == "require":
            raise RuntimeError(
                f"ingest pipeline missing name={name} "
                f"(FROSTY_PIPELINE_MODE=require; install frosty-parse-* in the cluster)"
            )

        status, body = await self._request(
            "PUT",
            f"/_ingest/pipeline/{name}",
            json_body=_STUB_INGEST_PIPELINE,
        )
        if status not in (200, 201):
            raise RuntimeError(
                f"ingest pipeline put failed name={name} status={status} body={body}"
            )
        logger.info("Created stub ingest pipeline %s", name)

    async def ensure_ingest_pipelines(
        self, names: list[str] | None = None
    ) -> list[str]:
        """Ensure all required frosty-parse-* pipelines exist. Returns ensured names."""
        targets = list(names) if names is not None else required_ingest_pipelines()
        for name in targets:
            await self.ensure_ingest_pipeline(name)
        return targets
