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
              classify_batch.rb (hybrid)
                 │                    │
     metadata hit              metadata miss
                 │                    │
     local classify          POST /classify/batch
     + /ensure/batch               (message path)
     only if stream new               │
                 │                    │
                 └────────┬───────────┘
                          ▼
                 splash-classify :8080
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

**Steady-state HTTP profile:** metadata-rich Splunk traffic → almost no classify HTTP; ensure HTTP only on newly seen data streams.

---

## Already Fixed ✅

| Item | Where |
|------|--------|
| Batch classify (`/classify/batch` + Ruby buffer) | `sidecar/app.py`, `classify_batch.rb` |
| Hybrid metadata-local classify + `/ensure/batch` | `classify_batch.rb`, `classify_rules.json`, `app.py` |
| Shared metadata rules (Python + Logstash) | `sidecar/classify_rules.json` |
| Persistent HTTP keep-alive to classify | `classify_batch.rb` |
| HTTP outside buffer mutex | `classify_batch.rb` |
| `flush_ms` flusher thread + tick input | `classify_batch.rb`, `logstash.conf` |
| Final flush on Logstash shutdown | `classify_batch.rb` `flush(final)` |
| Per-event isolation in batch classify | `sidecar/app.py` |
| Metadata classify `@lru_cache` (sidecar message path) | `sidecar/classify.py` |
| PUT-only data stream ensure | `sidecar/streams.py` |
| Template warm at sidecar startup | `sidecar/app.py` lifespan |
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
| Bounded `@buffer` / `@egress` + TCP backpressure | `classify_batch.rb` (`max_buffer` / `max_egress`; partial `take_batch`) |
| ES ensure lock not held across HTTP; in-flight coalesce | `sidecar/streams.py` |
| Coalesce waiters outlive template+stream cold path | `sidecar/streams.py` `_coalesce_wait_s` |
| Dedupe stream ensures per batch | `sidecar/app.py` |
| Short ES HTTP timeout (default 2s) | `sidecar/streams.py` `ELASTIC_HTTP_TIMEOUT_S` |

---

## Remaining Bottlenecks

### 🟡 1. Sync `httpx.Client` + uvicorn `--workers 4`

**File:** `sidecar/streams.py`, `sidecar/app.py`, `sidecar/Dockerfile`

Four workers each have their own `_ensured` cache (duplicate ES PUTs on cold streams). Less critical now that most traffic skips `/classify/batch`.

**Fix:** Prefer `--workers 1` (or AsyncClient + async endpoints).

### 🟡 2. Ensure/classify HTTP still serialized on one `Net::HTTP`

**File:** `classify_batch.rb` `@http_mutex`

First-seen streams and message-path batches still share one connection.

**Fix:** Connection pool, or single flusher-owned HTTP consumer.

### 🟢 3. Message-path still hits sidecar

Empty sourcetype/source events still use `/classify/batch` (intentional).

### 🟢 4. Exec tick is 1s resolution

Idle re-inject of message-path egress can wait up to ~1s.

### 🟢 5. S2S decoder copies whole buffer per frame

**File:** `s2s/s2s/decoder.py` `bytes(self._buf)`

---

## Priority Summary

| # | Item | Impact | Status |
|---|------|--------|--------|
| — | Hybrid metadata-local + ensure/batch | 🔴 High | ✅ Fixed |
| — | Batch classify / keep-alive / bounded buffers | 🔴 High | ✅ Fixed |
| — | Unlock ES ensure + coalesce waits | 🔴 High | ✅ Fixed |
| — | S2S upstream reliability | 🔴 High | ✅ Fixed |
| 1 | Uvicorn workers / shared cache | 🟡 Medium | Open |
| 2 | HTTP pool for ensure/classify | 🟡 Medium | Open |
| 3 | Async ES client | 🟡 Medium | Open |
| 4 | Sub-second idle tick | 🟢 Low | Open |

## Recommended next steps

1. Drop classify to `--workers 1`
2. Unblock Logstash→sidecar HTTP (pool or single consumer)
3. Optionally replace exec tick with a sub-second heartbeat if idle message-path latency matters

## Smoke checklist

- Event with `sourcetype=access_combined` → local classify; one `/ensure/batch` then zero sidecar classify HTTP
- Same stream again → zero HTTP
- Empty sourcetype/source + access log `message` → `/classify/batch` still used
