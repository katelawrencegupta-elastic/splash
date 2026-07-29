"""HTTP classify sidecar: classify events and ensure ECS data streams."""

from __future__ import annotations

import asyncio
import hmac
import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from classify import EventKind, classify_event, data_stream_name, parser_pipeline_name
from streams import DataStreamManager

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("splash.classify")

# Required — no hardcoded cluster default (avoids silent wrong-cluster writes).
ELASTIC_HOST = os.environ.get("ELASTIC_HOST", "").strip().rstrip("/")
ELASTIC_API_KEY = os.environ.get("ELASTIC_API_KEY", "").strip()
DATA_STREAM_NAMESPACE = os.environ.get("DATA_STREAM_NAMESPACE", "default")
ELASTIC_ENSURE_CONCURRENCY = max(
    1, int(os.environ.get("ELASTIC_ENSURE_CONCURRENCY", "8"))
)
FROSTY_PIPELINE_MODE = os.environ.get("FROSTY_PIPELINE_MODE", "require").strip().lower()
CLASSIFY_AUTH_TOKEN = os.environ.get("CLASSIFY_AUTH_TOKEN", "").strip()
CLASSIFY_AUTH_DISABLED = os.environ.get("CLASSIFY_AUTH_DISABLED", "").strip().lower() in {
    "1",
    "true",
    "yes",
}

_manager: Optional[DataStreamManager] = None
_manager_lock = threading.Lock()
_pipelines_ready = False
_ensure_failures = 0
_ensure_failures_lock = threading.Lock()
_classify_batch_requests = 0
_classify_batch_events = 0
_ensure_batch_requests = 0
_ensure_batch_streams = 0
_http_requests: dict[str, int] = {}
_metrics_counters_lock = threading.Lock()


def get_manager() -> DataStreamManager:
    """Return the process-wide DataStreamManager (single uvicorn worker)."""
    global _manager
    if _manager is not None:
        return _manager
    with _manager_lock:
        if _manager is None:
            if not ELASTIC_HOST:
                raise HTTPException(status_code=500, detail="ELASTIC_HOST is not set")
            if not ELASTIC_API_KEY:
                raise HTTPException(status_code=500, detail="ELASTIC_API_KEY is not set")
            _manager = DataStreamManager(
                ELASTIC_HOST,
                ELASTIC_API_KEY,
                frosty_pipeline_mode=FROSTY_PIPELINE_MODE,
            )
            logger.info(
                "DataStreamManager initialized for %s frosty_mode=%s",
                ELASTIC_HOST,
                FROSTY_PIPELINE_MODE,
            )
        return _manager


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    global _pipelines_ready
    if not ELASTIC_HOST:
        raise RuntimeError("ELASTIC_HOST is required (set it in the environment)")
    if not ELASTIC_API_KEY:
        raise RuntimeError("ELASTIC_API_KEY is required (set it in the environment)")
    manager = get_manager()
    try:
        await manager.ensure_template()
    except Exception:
        logger.exception(
            "Failed to ensure index template at startup; will retry on demand"
        )
    try:
        ensured = await manager.ensure_ingest_pipelines()
        _pipelines_ready = True
        logger.info("Ingest pipelines ready: %s", ", ".join(ensured))
    except Exception:
        _pipelines_ready = False
        logger.exception(
            "Failed to ensure frosty-parse-* ingest pipelines; /health will be 503 "
            "until ensure succeeds"
        )
    yield
    global _manager
    _pipelines_ready = False
    with _manager_lock:
        if _manager is not None:
            await _manager.close()
            _manager = None
            logger.info("Closed DataStreamManager HTTP client")


app = FastAPI(title="splash-classify", version="1.0.0", lifespan=lifespan)

# Paths that stay open for k8s probes / Prometheus (no Bearer).
_AUTH_SKIP_PATHS = frozenset({"/health", "/metrics"})


class ClassifyAuthMiddleware(BaseHTTPMiddleware):
    """Require Bearer CLASSIFY_AUTH_TOKEN on mutating classify routes."""

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path.rstrip("/") or "/"
        # Normalize trailing slash; skip probes/metrics only.
        if path in _AUTH_SKIP_PATHS or path == "/":
            return await call_next(request)
        if CLASSIFY_AUTH_DISABLED:
            return await call_next(request)
        if not CLASSIFY_AUTH_TOKEN:
            return JSONResponse(
                status_code=503,
                content={"detail": "CLASSIFY_AUTH_TOKEN is not configured"},
            )
        authorization = request.headers.get("authorization") or request.headers.get(
            "Authorization"
        )
        if not authorization or not authorization.startswith("Bearer "):
            return JSONResponse(
                status_code=401,
                content={"detail": "missing bearer token"},
            )
        provided = authorization[len("Bearer ") :].strip()
        if not hmac.compare_digest(provided, CLASSIFY_AUTH_TOKEN):
            return JSONResponse(
                status_code=401,
                content={"detail": "invalid bearer token"},
            )
        return await call_next(request)


app.add_middleware(ClassifyAuthMiddleware)


class ClassifyRequest(BaseModel):
    sourcetype: str = ""
    source: str = ""
    message: str = ""
    splunk_index: str = Field(default="", alias="splunk_index")

    class Config:
        populate_by_name = True


class ClassifyResponse(BaseModel):
    kind: str
    dataset: str
    namespace: str
    data_stream: str
    pipeline_name: str
    reason: str
    fallback: bool = False


class BatchClassifyRequest(BaseModel):
    events: list[ClassifyRequest]


class BatchClassifyResponse(BaseModel):
    results: list[ClassifyResponse]


class EnsureBatchRequest(BaseModel):
    streams: list[str]


class EnsureStreamResult(BaseModel):
    data_stream: str
    ok: bool
    fallback: bool = False
    resolved_stream: str


class EnsureBatchResponse(BaseModel):
    results: list[EnsureStreamResult]


def _fallback_response(*, reason: str) -> ClassifyResponse:
    namespace = DATA_STREAM_NAMESPACE or "default"
    return ClassifyResponse(
        kind="generic",
        dataset="generic",
        namespace=namespace,
        data_stream=f"logs-generic-{namespace}",
        pipeline_name=parser_pipeline_name(EventKind.GENERIC),
        reason=reason,
        fallback=True,
    )


def _fallback_stream_name() -> str:
    namespace = DATA_STREAM_NAMESPACE or "default"
    return f"logs-generic-{namespace}"


def _classify_fields(req: ClassifyRequest) -> ClassifyResponse:
    classified = classify_event(
        sourcetype=req.sourcetype or "",
        source=req.source or "",
        message=req.message or "",
        splunk_index=(req.splunk_index or "").strip(),
    )
    namespace = DATA_STREAM_NAMESPACE
    stream = data_stream_name(classified.dataset, namespace)
    return ClassifyResponse(
        kind=classified.kind.value,
        dataset=classified.dataset,
        namespace=namespace,
        data_stream=stream,
        pipeline_name=classified.pipeline_name,
        reason=classified.reason,
        fallback=False,
    )


async def _ensure_one(manager: DataStreamManager, stream: str) -> bool:
    global _ensure_failures
    try:
        await manager.ensure_data_stream(stream)
        return True
    except Exception:
        with _ensure_failures_lock:
            _ensure_failures += 1
        logger.exception("Failed to ensure data stream %s; falling back", stream)
        return False


async def _ensure_stream_or_fallback(result: ClassifyResponse) -> ClassifyResponse:
    """Ensure the target stream; on failure, downgrade this event only."""
    manager = get_manager()
    if await _ensure_one(manager, result.data_stream):
        return result
    fallback = _fallback_response(reason="fallback=ensure_failed:RuntimeError")
    await _ensure_one(manager, fallback.data_stream)
    return fallback


async def _ensure_many(manager: DataStreamManager, streams: list[str]) -> dict[str, bool]:
    """Ensure streams concurrently, capped by ELASTIC_ENSURE_CONCURRENCY."""
    sem = asyncio.Semaphore(ELASTIC_ENSURE_CONCURRENCY)
    ok: dict[str, bool] = {}

    async def one(stream: str) -> None:
        async with sem:
            ok[stream] = await _ensure_one(manager, stream)

    await asyncio.gather(*(one(s) for s in streams))
    return ok


async def _ensure_unique_streams(
    results: list[ClassifyResponse],
) -> list[ClassifyResponse]:
    """Ensure each distinct data_stream once, then map failures to fallback."""
    manager = get_manager()
    unique = list({r.data_stream for r in results})
    ok = await _ensure_many(manager, unique)

    if all(ok.values()):
        return results

    fallback_stream = _fallback_stream_name()
    await _ensure_one(manager, fallback_stream)

    out: list[ClassifyResponse] = []
    for result in results:
        if ok.get(result.data_stream, False):
            out.append(result)
        else:
            out.append(
                _fallback_response(reason="fallback=ensure_failed:RuntimeError")
            )
    return out


async def _ensure_streams_batch(streams: list[str]) -> list[EnsureStreamResult]:
    """Ensure each distinct stream once; failures resolve to the generic fallback."""
    manager = get_manager()
    ordered_unique: list[str] = []
    seen: set[str] = set()
    for name in streams:
        stream = (name or "").strip()
        if not stream or stream in seen:
            continue
        seen.add(stream)
        ordered_unique.append(stream)

    ok = await _ensure_many(manager, ordered_unique)

    if not all(ok.values()):
        await _ensure_one(manager, _fallback_stream_name())

    fallback_name = _fallback_stream_name()
    results: list[EnsureStreamResult] = []
    for stream in ordered_unique:
        if ok.get(stream, False):
            results.append(
                EnsureStreamResult(
                    data_stream=stream,
                    ok=True,
                    fallback=False,
                    resolved_stream=stream,
                )
            )
        else:
            results.append(
                EnsureStreamResult(
                    data_stream=stream,
                    ok=False,
                    fallback=True,
                    resolved_stream=fallback_name,
                )
            )
    return results


@app.get("/health")
async def health() -> dict[str, object]:
    """Liveness + pipeline readiness. Returns 503 until frosty-parse-* are ensured."""
    global _pipelines_ready
    if not _pipelines_ready:
        # Retry ensure so a transient ES outage at boot self-heals without restart.
        try:
            manager = get_manager()
            ensured = await manager.ensure_ingest_pipelines()
            _pipelines_ready = True
            logger.info("Ingest pipelines ready (health retry): %s", ", ".join(ensured))
        except Exception:
            logger.exception("Ingest pipeline ensure still failing")
            raise HTTPException(
                status_code=503,
                detail={
                    "status": "unavailable",
                    "pipelines_ready": False,
                    "frosty_pipeline_mode": FROSTY_PIPELINE_MODE,
                    "reason": "frosty-parse ingest pipelines not ensured",
                },
            ) from None

    return {
        "status": "ok",
        "pipelines_ready": True,
        "frosty_pipeline_mode": FROSTY_PIPELINE_MODE,
    }


@app.get("/metrics")
async def metrics() -> PlainTextResponse:
    """Prometheus text exposition for classify readiness and request counters."""
    ready = 1 if _pipelines_ready else 0
    with _ensure_failures_lock:
        failures = _ensure_failures
    with _metrics_counters_lock:
        batch_reqs = _classify_batch_requests
        batch_events = _classify_batch_events
        ensure_reqs = _ensure_batch_requests
        ensure_streams = _ensure_batch_streams
        http_counts = dict(_http_requests)
    lines = [
        "# HELP splash_pipelines_ready 1 if frosty-parse ingest pipelines are ready",
        "# TYPE splash_pipelines_ready gauge",
        f"splash_pipelines_ready {ready}",
        "# HELP splash_ensure_failures_total Data-stream ensure failures",
        "# TYPE splash_ensure_failures_total counter",
        f"splash_ensure_failures_total {failures}",
        "# HELP splash_frosty_mode_info Frosty pipeline mode (1=active label)",
        "# TYPE splash_frosty_mode_info gauge",
        f'splash_frosty_mode_info{{mode="{FROSTY_PIPELINE_MODE}"}} 1',
        "# HELP splash_classify_batch_requests_total POST /classify/batch calls",
        "# TYPE splash_classify_batch_requests_total counter",
        f"splash_classify_batch_requests_total {batch_reqs}",
        "# HELP splash_classify_batch_events_total Events in /classify/batch (metadata-miss path)",
        "# TYPE splash_classify_batch_events_total counter",
        f"splash_classify_batch_events_total {batch_events}",
        "# HELP splash_ensure_batch_requests_total POST /ensure/batch calls",
        "# TYPE splash_ensure_batch_requests_total counter",
        f"splash_ensure_batch_requests_total {ensure_reqs}",
        "# HELP splash_ensure_batch_streams_total Streams in /ensure/batch",
        "# TYPE splash_ensure_batch_streams_total counter",
        f"splash_ensure_batch_streams_total {ensure_streams}",
        "# HELP splash_classify_http_requests_total Classify HTTP requests by path",
        "# TYPE splash_classify_http_requests_total counter",
    ]
    for path, count in sorted(http_counts.items()):
        lines.append(f'splash_classify_http_requests_total{{path="{path}"}} {count}')
    lines.append("")
    return PlainTextResponse("\n".join(lines), media_type="text/plain; version=0.0.4")


def _bump_http(path: str) -> None:
    with _metrics_counters_lock:
        _http_requests[path] = _http_requests.get(path, 0) + 1


@app.post("/classify", response_model=ClassifyResponse)
async def classify(req: ClassifyRequest) -> ClassifyResponse:
    _bump_http("/classify")
    try:
        result = _classify_fields(req)
    except Exception as exc:
        logger.exception("classify failed for single event")
        raise HTTPException(status_code=500, detail=f"classify failed: {exc}") from exc
    return await _ensure_stream_or_fallback(result)


@app.post("/classify/batch", response_model=BatchClassifyResponse)
async def classify_batch(req: BatchClassifyRequest) -> BatchClassifyResponse:
    _bump_http("/classify/batch")
    global _classify_batch_requests, _classify_batch_events
    with _metrics_counters_lock:
        _classify_batch_requests += 1
        _classify_batch_events += len(req.events)
    if not req.events:
        return BatchClassifyResponse(results=[])
    if len(req.events) > 2000:
        raise HTTPException(status_code=400, detail="batch too large (max 2000)")

    classified: list[ClassifyResponse] = []
    for event in req.events:
        try:
            classified.append(_classify_fields(event))
        except Exception:
            logger.exception("classify failed for one event in batch; isolating")
            classified.append(_fallback_response(reason="fallback=classify_error"))

    return BatchClassifyResponse(results=await _ensure_unique_streams(classified))


@app.post("/ensure/batch", response_model=EnsureBatchResponse)
async def ensure_batch(req: EnsureBatchRequest) -> EnsureBatchResponse:
    _bump_http("/ensure/batch")
    global _ensure_batch_requests, _ensure_batch_streams
    with _metrics_counters_lock:
        _ensure_batch_requests += 1
        _ensure_batch_streams += len(req.streams)
    if not req.streams:
        return EnsureBatchResponse(results=[])
    if len(req.streams) > 2000:
        raise HTTPException(status_code=400, detail="batch too large (max 2000)")
    return EnsureBatchResponse(results=await _ensure_streams_batch(req.streams))
