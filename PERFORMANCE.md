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
              classify_batch.rb (hybrid + HTTP pool)
                 │                    │
     metadata hit              metadata miss
                 │                    │
     local classify          POST /classify/batch
     + /ensure/batch              (message path)
     only if stream new            │
                 │                    │
                 └────────┬───────────┘
                          ▼
                 splash-classify :8080 (workers=1, async)
                 (policy + ES stream ensure)
                          │
                          ▼
                 Elasticsearch Cloud
                 logs-{dataset}-{namespace}
```

**Data flow (cooked):**
1. Splunk sends cooked S2S to `s2s-decode:39998`
2. Decoder emits NDJSON to Logstash `:39996` (batched upstream writes)
3. Logstash runs shared metadata rules from `classify_rules.json`
   - **Hit:** set ECS fields locally; `POST /ensure/batch` only the first time a stream is seen
   - **Miss:** buffer and `POST /classify/batch` (message-pattern / generic)
4. Logstash indexes into `[@metadata][target_stream]`

**Steady-state HTTP profile:** metadata-rich Splunk traffic → almost no classify HTTP; ensure HTTP only on newly seen data streams. Logstash uses a keep-alive HTTP pool (`CLASSIFY_HTTP_POOL`, default 4); classify runs as a single async uvicorn worker.

---

## Already Fixed

| Item | Where |
|------|--------|
| Batch classify (`/classify/batch` + Ruby buffer) | `sidecar/app.py`, `classify_batch.rb` |
| Hybrid metadata-local classify + `/ensure/batch` | `classify_batch.rb`, `classify_rules.json`, `app.py` |
| Shared metadata rules (Python + Logstash) | `sidecar/classify_rules.json` |
| Persistent HTTP keep-alive pool to classify | `classify_batch.rb` (`CLASSIFY_HTTP_POOL`) |
| HTTP outside buffer mutex | `classify_batch.rb` |
| Uvicorn `--workers 1` (unified caches) | `sidecar/Dockerfile` `UVICORN_WORKERS` |
| Async `httpx.AsyncClient` + async ensure/classify | `streams.py`, `app.py` |
| Parallel stream ensures per batch (semaphore) | `app.py` `ELASTIC_ENSURE_CONCURRENCY` |
| `flush_ms` flusher thread + tick input | `classify_batch.rb`, `logstash.conf` |
| Final flush on Logstash shutdown | `classify_batch.rb` `flush(final)` |
| Per-event isolation in batch classify | `sidecar/app.py` |
| Metadata classify `@lru_cache` (sidecar message path) | `sidecar/classify.py` |
| PUT-only data stream ensure | `sidecar/streams.py` |
| Template warm at sidecar startup | `sidecar/app.py` lifespan |
| `DataStreamManager.close()` on shutdown | `sidecar/app.py` lifespan |
| `stdout rubydebug` removed | `logstash.conf` |
| `pipeline.batch.size: 500` | `logstash.yml` |
| S2S upstream: inflight retry, batching, backpressure | `s2s/server.py` |
| S2S graceful drain on shutdown | `s2s/server.py` |
| Elasticsearch host from env only (fail-fast) | `sidecar/app.py`, compose |
| Cooked S2S via `s2s-decode` only | compose + Dockerfile |
| Bounded `@buffer` / `@egress` + TCP backpressure | `classify_batch.rb` |
| ES ensure lock not held across HTTP; in-flight coalesce | `sidecar/streams.py` |
| Coalesce waiters outlive template+stream cold path | `sidecar/streams.py` `_coalesce_wait_s` |
| Short ES HTTP timeout (default 2s) | `sidecar/streams.py` `ELASTIC_HTTP_TIMEOUT_S` |

---

## Remaining Bottlenecks

### Low: Exec tick is 1s resolution

**File:** `logstash/pipeline/logstash.conf`

Idle re-inject of message-path egress can wait up to ~1s.

### Low: S2S decoder copies whole buffer per frame

**File:** `s2s/s2s/decoder.py` `bytes(self._buf)`

### Low: Message-path still hits sidecar

Empty sourcetype/source events still use `/classify/batch` (intentional).

---

## Priority Summary

| # | Item | Impact | Status |
|---|------|--------|--------|
| — | Hybrid metadata-local + ensure/batch | High | Fixed |
| — | HTTP pool + workers=1 + async ES | Medium | Fixed |
| — | Bounded buffers / coalesce waits / S2S upstream | High | Fixed |
| 1 | Sub-second idle tick | Low | Open |
| 2 | S2S decoder buffer copy | Low | Open |

## Recommended next steps

1. Optionally replace exec tick with a sub-second heartbeat if idle message-path latency matters
2. Parse S2S frames from `memoryview` / offset APIs

## Smoke checklist

- Event with `sourcetype=access_combined` → local classify; one `/ensure/batch` then zero sidecar classify HTTP
- Same stream again → zero HTTP
- Empty sourcetype/source + access log `message` → `/classify/batch` still used
- Burst of new streams → concurrent pool connections; no 4× duplicate template ensures
