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
    classify_health_url: str = "http://127.0.0.1:8080/health"
    logstash_stats_url: str = "http://127.0.0.1:9600/_node/stats"
    elastic_host: str = ""
    elastic_api_key: str = ""
    namespace: str = "loadtest"
    interval_s: float = 5.0


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
        s2s = await self._get_json(self.config.s2s_health_url)
        classify = await self._get_json(self.config.classify_health_url)
        ls = await self._get_json(self.config.logstash_stats_url)
        es_count = await self._es_count()

        s2s_stats = (s2s or {}).get("stats") if isinstance(s2s, dict) else None
        if not isinstance(s2s_stats, dict):
            s2s_stats = {}

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
            "s2s_events_emitted": s2s_stats.get("events_emitted"),
            "s2s_bytes_consumed": s2s_stats.get("bytes_consumed"),
            "s2s_upstream_queue": s2s_stats.get("upstream_queue"),
            "s2s_frames_ok": s2s_stats.get("frames_ok"),
            "classify_ok": isinstance(classify, dict) and classify.get("status") == "ok",
            "classify_pipelines_ready": isinstance(classify, dict)
            and classify.get("pipelines_ready") is True,
            "ls_events_in": events_in,
            "ls_events_out": events_out,
            "es_count": es_count,
            "s2s_error": (s2s or {}).get("_error") if isinstance(s2s, dict) else None,
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
