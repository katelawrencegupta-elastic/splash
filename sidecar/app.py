"""HTTP classify sidecar: classify events and ensure ECS data streams."""

from __future__ import annotations

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

_manager: Optional[DataStreamManager] = None
_manager_lock = threading.Lock()


def get_manager() -> DataStreamManager:
    """Return the process-wide DataStreamManager (safe under uvicorn threadpool)."""
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
    # Eager init so misconfig fails at startup, not on first event.
    get_manager()
    yield
    global _manager
    with _manager_lock:
        if _manager is not None:
            _manager.close()
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


def _ensure_stream_or_fallback(result: ClassifyResponse) -> ClassifyResponse:
    """Ensure the target stream; on failure, downgrade this event only."""
    try:
        get_manager().ensure_data_stream(result.data_stream)
        return result
    except Exception as exc:
        logger.exception(
            "Failed to ensure data stream %s; falling back", result.data_stream
        )
        fallback = _fallback_response(
            reason=f"fallback=ensure_failed:{type(exc).__name__}"
        )
        try:
            get_manager().ensure_data_stream(fallback.data_stream)
        except Exception:
            logger.exception("Failed to ensure fallback stream %s", fallback.data_stream)
        return fallback


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "elastic_host": ELASTIC_HOST}


@app.post("/classify", response_model=ClassifyResponse)
def classify(req: ClassifyRequest) -> ClassifyResponse:
    try:
        result = _classify_fields(req)
    except Exception as exc:
        logger.exception("classify failed for single event")
        raise HTTPException(status_code=500, detail=f"classify failed: {exc}") from exc
    return _ensure_stream_or_fallback(result)


@app.post("/classify/batch", response_model=BatchClassifyResponse)
def classify_batch(req: BatchClassifyRequest) -> BatchClassifyResponse:
    if not req.events:
        return BatchClassifyResponse(results=[])
    if len(req.events) > 2000:
        raise HTTPException(status_code=400, detail="batch too large (max 2000)")

    results: list[ClassifyResponse] = []
    for event in req.events:
        try:
            result = _classify_fields(event)
        except Exception:
            logger.exception("classify failed for one event in batch; isolating")
            result = _fallback_response(reason="fallback=classify_error")
        results.append(_ensure_stream_or_fallback(result))

    return BatchClassifyResponse(results=results)
