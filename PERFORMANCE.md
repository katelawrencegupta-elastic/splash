# Splash Performance Analysis

## Architecture

```
Splunk cooked tcpout :39998
        │
        ▼
  s2s-decode (Python)          Splunk uncooked :39997
  decode S2S → NDJSON                   │
        │                               ▼
        └──────────► Logstash :39996 / :39997
                            │
                            ▼
              classify_batch.rb (batched HTTP)
                            │
                            ▼
              splash-classify :8080
              (metadata cache + ECS stream ensure)
                            │
                            ▼
                    Elasticsearch Cloud
                    logs-{dataset}-{namespace}
```

**Data flow (cooked):**
1. Splunk sends cooked S2S to `s2s-decode:39998`
2. Decoder emits NDJSON to Logstash `:39996` (batched upstream writes)
3. Logstash buffers events and POSTs `/classify/batch`
4. Classify sidecar returns ECS fields + ensures data streams
5. Logstash indexes into the returned `logs-*` stream

---

## Already Fixed ✅

| Item | Where |
|------|--------|
| Batch classify (`/classify/batch` + Ruby buffer) | `sidecar/app.py`, `classify_batch.rb` |
| Persistent HTTP keep-alive to classify | `classify_batch.rb` |
| HTTP outside buffer mutex | `classify_batch.rb` |
| `flush_ms` flusher thread + tick input | `classify_batch.rb`, `logstash.conf` |
| Final flush on Logstash shutdown | `classify_batch.rb` `flush(final)` |
| Per-event isolation in batch classify | `sidecar/app.py` |
| Metadata classify `@lru_cache` | `sidecar/classify.py` |
| PUT-only data stream ensure | `sidecar/streams.py` |
| `DataStreamManager.close()` on shutdown | `sidecar/app.py` lifespan |
| Uvicorn `--workers 4` | `sidecar/Dockerfile` |
| Reused `httpx.Client` | `sidecar/streams.py` |
| `stdout rubydebug` removed | `logstash.conf` |
| `pipeline.batch.size: 500` | `logstash.yml` |
| S2S upstream: inflight retry, batching, backpressure | `s2s/server.py` |
| S2S graceful drain on shutdown | `s2s/server.py` |
| `_fill_batch` without per-item `wait_for` tasks | `s2s/server.py` |
| Elasticsearch host from env only (fail-fast) | `sidecar/app.py`, compose |
| Thread-safe `get_manager()` | `sidecar/app.py` |
| Cooked S2S via `s2s-decode` only (no Logstash s2s plugin) | compose + Dockerfile |
| Logstash healthcheck | `docker-compose.yml` |
| Classify fallback via boolean flag | `app.py` + `classify_batch.rb` |

---

## Remaining Bottlenecks

### 🟡 1. Sync `httpx.Client` inside an async FastAPI app

**File:** `sidecar/streams.py`, `sidecar/app.py`

Classify handlers are sync `def` (threadpool). `DataStreamManager` uses sync `httpx.Client`. Under heavy concurrent ensure-traffic this can saturate the thread pool.

**Fix:** `httpx.AsyncClient` + `async def` endpoints + `asyncio.Lock`.

> **Estimated impact:** Better concurrency when many distinct streams are ensured at once.

### 🟢 2. Classify cache ignores message-only paths

**File:** `sidecar/classify.py`

`_classify_from_metadata` is cached; message-pattern classification is not (by design — high cardinality). Steady streams with empty sourcetype/source still pay regex cost per event.

> **Estimated impact:** Low for typical Splunk metadata-rich traffic.

### 🟢 3. Exec tick is 1s resolution

**File:** `logstash/pipeline/logstash.conf`

The `_classify_tick` exec input wakes idle egress drain at 1s. The Ruby flusher thread still classifies on `flush_ms`, but pipeline re-injection while idle waits for the next tick or `periodic_flush`.

> **Estimated impact:** Up to ~1s idle latency before classified events leave the Ruby filter.

---

## Priority Summary

| # | Item | Impact | Status |
|---|------|--------|--------|
| — | Batch classify | 🔴 High | ✅ Fixed |
| — | HTTP keep-alive + mutex scope | 🔴 High | ✅ Fixed |
| — | Metadata classify cache | 🟢 Low | ✅ Fixed |
| — | PUT-only stream ensure | 🟢 Low | ✅ Fixed |
| — | Manager `close()` | 🟢 Low | ✅ Fixed |
| — | Uvicorn workers | 🟡 Medium | ✅ Fixed |
| — | S2S upstream reliability | 🔴 High | ✅ Fixed |
| 1 | Async ES client | 🟡 Medium | Open |
| 2 | Message-path cache | 🟢 Low | Open (intentional) |
| 3 | Sub-second idle tick | 🟢 Low | Open |

## Recommended next steps

1. Switch classify/stream ensure to `AsyncClient` if concurrent stream creation becomes hot
2. Optionally replace exec tick with a sub-second heartbeat if idle latency matters
