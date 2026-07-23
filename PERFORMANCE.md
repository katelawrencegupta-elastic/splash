# Splash Performance Analysis

## Architecture

Splash is a Splunk → Elasticsearch log migration pipeline with three components:

```
Splunk (raw TCP) → Logstash :39997 → classify sidecar (FastAPI :8080) → Elasticsearch Cloud
```

**Data flow per event:**
1. Splunk forwards raw TCP events to Logstash
2. Logstash normalizes fields (sourcetype, source, message, splunk_index)
3. Logstash makes a synchronous HTTP POST to the classify sidecar **per event**
4. Classify sidecar returns ECS metadata (dataset, data_stream, pipeline_name)
5. Logstash writes the tagged event to the correct Elasticsearch data stream

---

## Already Fixed ✅

| Item | Where |
|------|--------|
| `stdout rubydebug` removed from output | logstash.conf |
| `pipeline.workers` now omitted (defaults to CPU cores) | logstash.yml |
| `pipeline.batch.size` raised to 500 | logstash.yml |
| `httpx.Client` created once and reused in `DataStreamManager` | streams.py |

---

## Remaining Bottlenecks

### 🔴 1. One HTTP call to the classify sidecar per event — no batching

**File:** `logstash/pipeline/logstash.conf` (lines 55–69)

The `http` filter makes a blocking HTTP POST for every single event. This is the dominant bottleneck at any real log volume. Every event pays the full round-trip: TCP send, FastAPI parse, classify, response, Logstash parse. With `pipeline.batch.size: 500`, Logstash accumulates batches internally but then fires 500 individual HTTP requests in sequence through each worker.

**Fix:** Add a `/classify/batch` endpoint to the sidecar. The classify logic is pure and stateless — trivial to vectorize. Then use Logstash's `http` filter once per pipeline flush rather than per event (requires the aggregate filter or a custom Ruby script to collect the batch).

```python
# sidecar/app.py — add batch endpoint
class BatchClassifyRequest(BaseModel):
    events: list[ClassifyRequest]

class BatchClassifyResponse(BaseModel):
    results: list[ClassifyResponse]

@app.post("/classify/batch", response_model=BatchClassifyResponse)
def classify_batch(req: BatchClassifyRequest) -> BatchClassifyResponse:
    results = []
    for event in req.events:
        classified = classify_event(
            sourcetype=event.sourcetype or "",
            source=event.source or "",
            message=event.message or "",
            splunk_index=(event.splunk_index or "").strip(),
        )
        stream = data_stream_name(classified.dataset, DATA_STREAM_NAMESPACE)
        get_manager().ensure_data_stream(stream)
        results.append(ClassifyResponse(
            kind=classified.kind.value,
            dataset=classified.dataset,
            namespace=DATA_STREAM_NAMESPACE,
            data_stream=stream,
            pipeline_name=classified.pipeline_name,
            reason=classified.reason,
        ))
    return BatchClassifyResponse(results=results)
```

> **Estimated impact:** 5–20× throughput improvement depending on event rate and batch size.

---

### 🟡 2. Sync `httpx.Client` inside an async FastAPI app

**File:** `sidecar/streams.py`, `sidecar/app.py`

The `classify` endpoint is `def` (sync), which FastAPI runs in a thread pool. The `DataStreamManager` uses a synchronous `httpx.Client`. This works, but each thread pool call blocks a thread for the full Elasticsearch round-trip. Under concurrent load from multiple Logstash workers, this thread pool can become a bottleneck.

**Fix:** Switch `DataStreamManager` to `httpx.AsyncClient` and make the endpoint `async def`:

```python
# streams.py
import httpx

class DataStreamManager:
    def __init__(self, elastic_host: str, api_key: str) -> None:
        ...
        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers=self._headers,
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
        )
        self._lock = asyncio.Lock()   # swap threading.RLock for asyncio.Lock

    async def _request(self, method, path, *, json_body=None):
        url = f"{self._host}{path}"
        resp = await self._client.request(method, url, json=json_body)
        ...

    async def ensure_data_stream(self, name: str) -> None:
        if name in self._ensured:
            return
        async with self._lock:
            ...
```

```python
# app.py
@app.post("/classify", response_model=ClassifyResponse)
async def classify(req: ClassifyRequest) -> ClassifyResponse:
    ...
    await get_manager().ensure_data_stream(stream)
    ...
```

> **Estimated impact:** Better concurrency under load; eliminates thread pool saturation when multiple Logstash workers call the sidecar simultaneously.

---

### 🟡 3. Uvicorn running single-process

**File:** `sidecar/Dockerfile` (CMD line)

```dockerfile
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
```

No `--workers N` flag — single process. If Logstash has many pipeline workers, they all queue behind one event loop. This matters most before the batching fix (issue #1) is in place.

**Fix:**
```dockerfile
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "4"]
```

Note: with multiple workers, `_manager` is per-process (separate memory), which is fine — each worker independently caches its `_ensured` streams and Elasticsearch handles race conditions gracefully.

> **Estimated impact:** Linear throughput improvement under concurrent Logstash worker load.

---

### 🟢 4. Classify result caching

**File:** `sidecar/classify.py`

Most real-world log streams have low cardinality of (sourcetype, source, splunk_index) combinations. The `classify_event` function is deterministic and pure — repeated calls with the same inputs do the same regex work.

**Fix:**
```python
from functools import lru_cache

@lru_cache(maxsize=1024)
def _classify_cached(sourcetype: str, source: str, splunk_index: str) -> ClassifiedEvent:
    # Only cache when sourcetype or source is set — message content is high-cardinality
    return classify_event(sourcetype=sourcetype, source=source, splunk_index=splunk_index)
```

In `app.py`, use `_classify_cached` when `sourcetype` or `source` is non-empty, fall back to `classify_event` for message-pattern classification.

> **Estimated impact:** Eliminates repeated regex evaluation for steady-state log streams.

---

### 🟢 5. GET before PUT for data stream creation

**File:** `sidecar/streams.py` (`ensure_data_stream`)

Two Elasticsearch API calls (GET then PUT) for the first time a stream is seen. Since `resource_already_exists_exception` is already handled on PUT, the GET is redundant.

**Fix:**
```python
def ensure_data_stream(self, name: str) -> None:
    if name in self._ensured:
        return
    with self._lock:
        if name in self._ensured:
            return
        self.ensure_template()
        status, body = self._request("PUT", f"/_data_stream/{name}")
        if status in (200, 201):
            self._ensured.add(name)
            return
        if status == 400 and isinstance(body, dict):
            if body.get("error", {}).get("type") == "resource_already_exists_exception":
                self._ensured.add(name)
                return
        raise RuntimeError(f"data stream create failed name={name} status={status} body={body}")
```

> **Estimated impact:** Halves Elasticsearch API calls during startup/cold path. Negligible at steady state (cache hit).

---

## Priority Summary

| # | Bottleneck | Impact | Effort | Status |
|---|-----------|--------|--------|--------|
| 1 | Per-event HTTP classify — no batching | 🔴 Very High | Medium | Open |
| 2 | Sync httpx.Client in async FastAPI | 🟡 Medium | Medium | Open |
| 3 | Uvicorn single-process | 🟡 Medium | Trivial | Open |
| 4 | No classify result caching | 🟢 Low | Low | Open |
| 5 | GET before PUT for data stream | 🟢 Low | Low | Open |
| — | stdout rubydebug in output | 🔴 High | — | ✅ Fixed |
| — | pipeline.workers hardcoded to 2 | 🟡 Medium | — | ✅ Fixed |
| — | pipeline.batch.size too small | 🟡 Medium | — | ✅ Fixed |
| — | New httpx.Client per ES call | 🟡 Medium | — | ✅ Fixed |

## Recommended next steps

1. **Ship the batch classify endpoint** (issue #1) — this is by far the highest leverage change
2. **Add `--workers 4`** to uvicorn CMD (issue #3) — one-liner, do it now
3. **Switch to AsyncClient + async endpoints** (issue #2) — pairs naturally with the batch work
