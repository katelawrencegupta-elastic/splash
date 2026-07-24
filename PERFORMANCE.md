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
              classify_batch.rb (bounded buffer + batch HTTP)
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
3. Logstash buffers events (bounded) and POSTs `/classify/batch` (message omitted when metadata present)
4. Classify sidecar returns ECS fields + ensures distinct data streams once per batch
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
| Bounded `@buffer` / `@egress` + TCP backpressure | `classify_batch.rb` (`max_buffer` / `max_egress`) |
| Omit full `message` when sourcetype/source present | `classify_batch.rb` `build_payload` |
| ES ensure lock not held across HTTP; in-flight coalesce | `sidecar/streams.py` |
| Dedupe stream ensures per batch | `sidecar/app.py` `_ensure_unique_streams` |
| Short ES HTTP timeout (default 2s) | `sidecar/streams.py` `ELASTIC_HTTP_TIMEOUT_S` |

---

## Remaining Bottlenecks

### 🟡 1. Sync `httpx.Client` + uvicorn `--workers 4`

**File:** `sidecar/streams.py`, `sidecar/app.py`, `sidecar/Dockerfile`

Classify handlers are sync `def` (threadpool). Four workers each have their own LRU + `_ensured` cache, so stream ensures and classify cache misses are duplicated. With a single Logstash HTTP client, extra workers add little throughput.

**Fix:** Prefer `--workers 1` (or AsyncClient + async endpoints). Share ensure state only if multi-worker is required.

> **Estimated impact:** Medium — better cache hit rate and fewer duplicate ES PUTs.

### 🟡 2. Classify HTTP still serialized on one `Net::HTTP`

**File:** `classify_batch.rb` `@http_mutex`

All Logstash pipeline workers share one keep-alive connection.

**Fix:** Connection pool, or single flusher-owned HTTP consumer with workers only enqueueing.

> **Estimated impact:** Medium — higher classify RPS when sidecar has spare capacity.

### 🟢 3. Classify cache ignores message-only paths

**File:** `sidecar/classify.py`

`_classify_from_metadata` is cached; message-pattern classification is not (by design — high cardinality). Steady streams with empty sourcetype/source still pay regex cost per event (now on a ≤512B prefix).

> **Estimated impact:** Low for typical Splunk metadata-rich traffic.

### 🟢 4. Exec tick is 1s resolution

**File:** `logstash/pipeline/logstash.conf`

The `_classify_tick` exec input wakes idle egress drain at 1s. The Ruby flusher thread still classifies on `flush_ms`, but pipeline re-injection while idle waits for the next tick or `periodic_flush`.

> **Estimated impact:** Up to ~1s idle latency before classified events leave the Ruby filter.

### 🟢 5. S2S decoder copies whole buffer per frame

**File:** `s2s/s2s/decoder.py` `bytes(self._buf)`

**Fix:** Parse from `memoryview` / offset APIs.

---

## Priority Summary

| # | Item | Impact | Status |
|---|------|--------|--------|
| — | Batch classify | 🔴 High | ✅ Fixed |
| — | HTTP keep-alive + mutex scope | 🔴 High | ✅ Fixed |
| — | Metadata classify cache | 🟢 Low | ✅ Fixed |
| — | PUT-only stream ensure | 🟢 Low | ✅ Fixed |
| — | Bounded classify buffers + backpressure | 🔴 High | ✅ Fixed |
| — | Omit message when metadata present | 🔴 High | ✅ Fixed |
| — | Unlock ES ensure + batch dedupe + 2s timeout | 🔴 High | ✅ Fixed |
| — | S2S upstream reliability | 🔴 High | ✅ Fixed |
| 1 | Uvicorn workers / shared cache | 🟡 Medium | Open |
| 2 | Classify HTTP pool | 🟡 Medium | Open |
| 3 | Async ES client | 🟡 Medium | Open |
| 4 | Message-path cache | 🟢 Low | Open (intentional) |
| 5 | Sub-second idle tick | 🟢 Low | Open |

## Recommended next steps

1. Drop classify to `--workers 1` (or move to async single process)
2. Unblock classify HTTP (pool or single consumer thread)
3. Optionally replace exec tick with a sub-second heartbeat if idle latency matters
