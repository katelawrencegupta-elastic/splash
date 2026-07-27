"""HTTP classify sidecar: classify events and ensure ECS data streams."""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from classify import classify_event, data_stream_name
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

_manager: Optional[DataStreamManager] = None
_manager_lock = threading.Lock()


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
            _manager = DataStreamManager(ELASTIC_HOST, ELASTIC_API_KEY)
            logger.info("DataStreamManager initialized for %s", ELASTIC_HOST)
        return _manager


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    if not ELASTIC_HOST:
        raise RuntimeError("ELASTIC_HOST is required (set it in the environment)")
    if not ELASTIC_API_KEY:
        raise RuntimeError("ELASTIC_API_KEY is required (set it in the environment)")
    manager = get_manager()
    try:
        await manager.ensure_template()
    except Exception:
        logger.exception("Failed to ensure index template at startup; will retry on demand")
    yield
    global _manager
    with _manager_lock:
        if _manager is not None:
            await _manager.close()
            _manager = None
            logger.info("Closed DataStreamManager HTTP client")


app = FastAPI(title="splash-classify", version="1.0.0", lifespan=lifespan)


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
        pipeline_name="frosty-parse-generic",
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
    try:
        await manager.ensure_data_stream(stream)
        return True
    except Exception:
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
def health() -> dict[str, str]:
    return {"status": "ok", "elastic_host": ELASTIC_HOST}


@app.post("/classify", response_model=ClassifyResponse)
async def classify(req: ClassifyRequest) -> ClassifyResponse:
    try:
        result = _classify_fields(req)
    except Exception as exc:
        logger.exception("classify failed for single event")
        raise HTTPException(status_code=500, detail=f"classify failed: {exc}") from exc
    return await _ensure_stream_or_fallback(result)


@app.post("/classify/batch", response_model=BatchClassifyResponse)
async def classify_batch(req: BatchClassifyRequest) -> BatchClassifyResponse:
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
    if not req.streams:
        return EnsureBatchResponse(results=[])
    if len(req.streams) > 2000:
        raise HTTPException(status_code=400, detail="batch too large (max 2000)")
    return EnsureBatchResponse(results=await _ensure_streams_batch(req.streams))
