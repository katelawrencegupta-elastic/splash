"""HTTP classify sidecar: classify events and ensure ECS data streams."""

from __future__ import annotations

import logging
import os
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from classify import classify_event, data_stream_name
from streams import DataStreamManager

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("splash.classify")

ELASTIC_HOST = os.environ.get(
    "ELASTIC_HOST",
    "https://klgsplashpoc-ba74ce.es.us-central1.gcp.elastic.cloud:443",
)
ELASTIC_API_KEY = os.environ.get("ELASTIC_API_KEY", "")
DATA_STREAM_NAMESPACE = os.environ.get("DATA_STREAM_NAMESPACE", "default")

app = FastAPI(title="splash-classify", version="1.0.0")
_manager: Optional[DataStreamManager] = None


def get_manager() -> DataStreamManager:
    global _manager
    if _manager is None:
        if not ELASTIC_API_KEY:
            raise HTTPException(status_code=500, detail="ELASTIC_API_KEY is not set")
        _manager = DataStreamManager(ELASTIC_HOST, ELASTIC_API_KEY)
    return _manager


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


class BatchClassifyRequest(BaseModel):
    events: list[ClassifyRequest]


class BatchClassifyResponse(BaseModel):
    results: list[ClassifyResponse]


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
    )


def _ensure_streams(streams: set[str]) -> None:
    manager = get_manager()
    for stream in streams:
        try:
            manager.ensure_data_stream(stream)
        except Exception as exc:
            logger.exception("Failed to ensure data stream %s", stream)
            raise HTTPException(
                status_code=502,
                detail=f"ensure_data_stream failed: {exc}",
            ) from exc


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/classify", response_model=ClassifyResponse)
def classify(req: ClassifyRequest) -> ClassifyResponse:
    result = _classify_fields(req)
    _ensure_streams({result.data_stream})
    return result


@app.post("/classify/batch", response_model=BatchClassifyResponse)
def classify_batch(req: BatchClassifyRequest) -> BatchClassifyResponse:
    if not req.events:
        return BatchClassifyResponse(results=[])
    if len(req.events) > 2000:
        raise HTTPException(status_code=400, detail="batch too large (max 2000)")

    results = [_classify_fields(event) for event in req.events]
    _ensure_streams({r.data_stream for r in results})
    return BatchClassifyResponse(results=results)
