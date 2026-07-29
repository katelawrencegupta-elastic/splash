"""Poll Splash health endpoints and optional Elasticsearch _count."""

from __future__ import annotations

import base64
import csv
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger("loadtest.observe")


def _api_key_header(api_key: str) -> str:
    raw = api_key.strip()
    if ":" in raw:
        return base64.b64encode(raw.encode("utf-8")).decode("ascii")
    return raw


@dataclass
class ObserverConfig:
    s2s_health_url: str = "http://127.0.0.1:8081/health"
    # Comma-separated extra s2s health URLs for multi-shard (aggregated into totals).
    s2s_health_urls: str = ""
    classify_health_url: str = "http://127.0.0.1:8080/health"
    logstash_stats_url: str = "http://127.0.0.1:9600/_node/stats"
    elastic_host: str = ""
    elastic_api_key: str = ""
    namespace: str = "loadtest"
    interval_s: float = 5.0

    def all_s2s_urls(self) -> list[str]:
        urls = [self.s2s_health_url]
        extra = [u.strip() for u in (self.s2s_health_urls or "").split(",") if u.strip()]
        for u in extra:
            if u not in urls:
                urls.append(u)
        return urls


@dataclass
class Observer:
    config: ObserverConfig
    out_csv: Path
    rows: list[dict[str, Any]] = field(default_factory=list)
    _client: httpx.AsyncClient | None = None
    _started: float = 0.0

    async def __aenter__(self) -> Observer:
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(3.0, connect=2.0))
        self._started = time.monotonic()
        self.out_csv.parent.mkdir(parents=True, exist_ok=True)
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        self.flush()

    async def _get_json(self, url: str) -> dict[str, Any] | None:
        assert self._client is not None
        try:
            resp = await self._client.get(url)
            if resp.status_code >= 400:
                return {"_error": resp.status_code, "_body": resp.text[:200]}
            return resp.json()
        except Exception as exc:
            logger.debug("GET %s failed: %s", url, exc)
            return {"_error": str(exc)}

    async def _es_count(self) -> int | None:
        host = (self.config.elastic_host or "").rstrip("/")
        key = self.config.elastic_api_key or ""
        if not host or not key:
            return None
        assert self._client is not None
        index = f"logs-*-{self.config.namespace}"
        url = f"{host}/{index}/_count"
        headers = {
            "Authorization": f"ApiKey {_api_key_header(key)}",
            "Content-Type": "application/json",
        }
        try:
            resp = await self._client.get(url, headers=headers)
            if resp.status_code >= 400:
                logger.warning(
                    "ES _count status=%s body=%s", resp.status_code, resp.text[:200]
                )
                return None
            return int(resp.json().get("count", 0))
        except Exception as exc:
            logger.warning("ES _count failed: %s", exc)
            return None

    async def sample(self, *, gen: dict[str, Any] | None = None) -> dict[str, Any]:
        s2s_urls = self.config.all_s2s_urls()
        s2s_bodies = [await self._get_json(u) for u in s2s_urls]
        classify = await self._get_json(self.config.classify_health_url)
        ls = await self._get_json(self.config.logstash_stats_url)
        es_count = await self._es_count()

        emitted = 0
        bytes_consumed = 0
        frames_ok = 0
        queue_sum = 0
        queue_max = 0
        s2s_errors: list[str] = []
        for body in s2s_bodies:
            if not isinstance(body, dict):
                continue
            if body.get("_error") is not None:
                s2s_errors.append(str(body.get("_error")))
                continue
            st = body.get("stats") if isinstance(body.get("stats"), dict) else {}
            emitted += int(st.get("events_emitted") or 0)
            bytes_consumed += int(st.get("bytes_consumed") or 0)
            frames_ok += int(st.get("frames_ok") or 0)
            q = int(st.get("upstream_queue") or 0)
            queue_sum += q
            queue_max = max(queue_max, q)

        events_in = events_out = None
        if isinstance(ls, dict) and "_error" not in ls:
            try:
                events_in = ls["pipelines"]["main"]["events"]["in"]
                events_out = ls["pipelines"]["main"]["events"]["out"]
            except (KeyError, TypeError):
                try:
                    pipelines = ls.get("pipelines") or {}
                    first = next(iter(pipelines.values()))
                    events_in = first["events"]["in"]
                    events_out = first["events"]["out"]
                except Exception:
                    pass

        row: dict[str, Any] = {
            "t_mono": round(time.monotonic() - self._started, 3),
            "ts": time.time(),
            "gen_sent_events": (gen or {}).get("sent_events"),
            "gen_sent_bytes": (gen or {}).get("sent_bytes"),
            "gen_eps": (gen or {}).get("eps"),
            "gen_errors": (gen or {}).get("errors"),
            "s2s_shard_count": len(s2s_urls),
            "s2s_events_emitted": emitted,
            "s2s_bytes_consumed": bytes_consumed,
            "s2s_upstream_queue": queue_sum,
            "s2s_upstream_queue_max": queue_max,
            "s2s_frames_ok": frames_ok,
            "classify_ok": isinstance(classify, dict) and classify.get("status") == "ok",
            "classify_pipelines_ready": isinstance(classify, dict)
            and classify.get("pipelines_ready") is True,
            "ls_events_in": events_in,
            "ls_events_out": events_out,
            "es_count": es_count,
            "s2s_error": ";".join(s2s_errors) if s2s_errors else None,
            "ls_error": (ls or {}).get("_error") if isinstance(ls, dict) else None,
        }
        self.rows.append(row)
        return row

    def flush(self) -> None:
        if not self.rows:
            return
        fields = list(self.rows[0].keys())
        with self.out_csv.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields)
            writer.writeheader()
            writer.writerows(self.rows)
