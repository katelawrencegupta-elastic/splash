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
| `flush_ms` flusher thread + heartbeat tick input | `classify_batch.rb`, `logstash.conf` |
| Final flush on Logstash shutdown | `classify_batch.rb` `flush(final)` |
| Per-event isolation in batch classify | `sidecar/app.py` |
| Metadata classify `@lru_cache` (sidecar message path) | `sidecar/classify.py` |
| PUT-only data stream ensure | `sidecar/streams.py` |
| Template warm at sidecar startup | `sidecar/app.py` lifespan |
| `DataStreamManager.close()` on shutdown | `sidecar/app.py` lifespan |
| `stdout rubydebug` removed | `logstash.conf` |
| `pipeline.batch.size: 500` | `logstash.yml` |
| S2S upstream: inflight retry, batching, backpressure | `packages/s2s-decode/s2s/server.py` |
| S2S graceful drain on shutdown | `packages/s2s-decode/s2s/server.py` |
| Elasticsearch host from env only (fail-fast) | `sidecar/app.py`, compose |
| Cooked S2S via `s2s-decode` only | compose + Dockerfile |
| Bounded `@buffer` / `@egress` + TCP backpressure | `classify_batch.rb` |
| ES ensure lock not held across HTTP; in-flight coalesce | `sidecar/streams.py` |
| Coalesce waiters outlive template+stream cold path | `sidecar/streams.py` `_coalesce_wait_s` |
| Short ES HTTP timeout (default 2s) | `sidecar/streams.py` `ELASTIC_HTTP_TIMEOUT_S` |

---

## Remaining Bottlenecks

### Low: Heartbeat tick is 1s resolution

**File:** `packages/logstash-pipeline/pipeline/logstash.conf` — `heartbeat` input (replaced shell `exec` tick).

Idle re-inject of message-path egress can wait up to ~1s. Sub-second interval is optional if latency matters.

### Low: Message-path still hits sidecar

Empty sourcetype/source events still use `/classify/batch` (intentional). Reduce via rules + Splunk props ([compute-optimize.md](docs/runbooks/compute-optimize.md) Phase 1).

---

## Compute baseline (Phase 0 — locked)

| Item | Value |
|------|-------|
| Hot S1 sustained | **~0.0087 GB/s** (~5k eps @ ~1.5 KB) |
| Planning floor | **0.008 GB/s / stack** |
| Saturator | **Logstash** (queue peg ⇒ LS/ES behind) |
| Per-stack shape | LS **4 CPU / workers 4**, s2s **2 CPU** |
| Scale path | Horizontal shards; vertical LS probe before raising floor |

Playbook: [`docs/runbooks/compute-optimize.md`](docs/runbooks/compute-optimize.md).  
Baseline capture: `./scripts/compute-optimize-baseline.sh`.

## Priority Summary

| # | Item | Impact | Status |
|---|------|--------|--------|
| — | Hybrid metadata-local + ensure/batch | High | Fixed |
| — | HTTP pool + workers=1 + async ES | Medium | Fixed |
| — | Bounded buffers / coalesce waits / S2S upstream | High | Fixed |
| 1 | Metadata hit rate (rules + Splunk fields) | High ($/GB) | Expanded rules + hit tags |
| 2 | LS vertical probe 6/6 / 8/8 | Med | Documented; defaults stay 4/4 |
| 3 | Sub-second idle tick | Low | Open (heartbeat @ 1s) |
| 4 | s2s micro-opts | Low | Deferred (not saturator) |

## Recommended next steps

1. Keep miss_fraction &lt; 0.1 on soaks (`splash:hit_fraction:1m`); use `splunk/props.conf.example`.
2. Run Phase 2 LS vertical matrix before changing Helm CPU/workers.
3. Optionally lower heartbeat `interval` below 1s if idle message-path latency matters.

## Smoke checklist

- Event with `sourcetype=access_combined` → local classify; one `/ensure/batch` then zero sidecar classify HTTP
- Same stream again → zero HTTP
- Empty sourcetype/source + access log `message` → `/classify/batch` still used
- Burst of new streams → concurrent pool connections; no 8× duplicate template ensures
