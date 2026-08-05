# Compute optimization playbook — Splash

Raise **GB/s per vCPU-dollar** with gated phases. Do not raise one stack above
~0.015 GB/s without a fresh ramp.

## Phase 0 — Baseline (locked from prior S1)

Hot cooked path (~1.5 KB), Helm-shaped stack (Logstash **4 CPU / workers 4**,
s2s **2 CPU**):

| Metric | Value | Source |
|--------|-------|--------|
| Sustained GB/s | **~0.0087** (plan floor **0.008**) | S1 loadtest PASS |
| Saturated component | **Logstash** (upstream queue pegs when LS/ES behind) | alerts + loadtest notes |
| s2s role | Rarely primary; decode + NDJSON upstream | CPU share under S1 |
| classify on S1 (hot) | Near-idle after first ensure | hybrid metadata-local |

**Re-baseline anytime the stack is up:**

```bash
./scripts/compute-optimize-baseline.sh
# Optional: S1 soak
# python -m loadtest run -s S1 --eps 5000 --duration 120
```

**Gate:**

| Observation | Next phase |
|-------------|------------|
| Classify CPU high on hot S1 | Phase 1 (rules / Splunk fields) |
| s2s CPU ≫ LS, queue not pegged | Phase 4 |
| Otherwise | Phase 1 → Phase 2 |

---

## Phase 1 — Metadata hit rate

- Expand shared [`sidecar/classify_rules.json`](../../sidecar/classify_rules.json)
  (sync Logstash + Helm copies; tests enforce).
- Prefer Splunk props so `sourcetype` / `source` arrive populated.
- Watch `splash:miss_fraction:1m` (target **&lt; 0.1**; alert at 0.25) and
  `splash:hit_fraction:1m`.
- Events tagged `_classify_metadata_hit` (local) vs miss path (batch to sidecar).

---

## Phase 2 — Logstash vertical probe

Keep defaults **4 CPU / workers 4 / heap 1g** until a variant wins.

| Variant | LS CPU | workers | Heap / mem limit |
|---------|--------|---------|------------------|
| A (current) | 4 | 4 | 1g / 2Gi |
| B | 6 | 6 | 1536m / 3Gi |
| C | 8 | 8 | 1536m / 4Gi |

Never set `workers` above the CPU limit. Keep `UVICORN_WORKERS=1`.

**Pass:** higher sustained GB/s, queue not pegged, no OOM/GC thrash, ES keeps up.

**Decide:** if B/C clearly reach ~0.010–0.012 GB/s → raise planning floor in
[sharding.md](sharding.md) + Helm values. Else **keep 0.008 / 4+4** (horizontal
shards remain the scale path).

**Status:** defaults unchanged (4/4). Run matrix before changing production.

---

## Phase 3 — Cold path (only if miss-bound)

If `miss_fraction` stays high or S2 saturates classify/Ruby:

1. Tune `CLASSIFY_BATCH_SIZE` / `CLASSIFY_FLUSH_MS` / `CLASSIFY_HTTP_POOL≈workers`.
2. Profile Ruby JSON + `rebuild_event` only with a flamegraph.
3. Scale classify **replicas**, never `UVICORN_WORKERS>1`.

**Status:** deferred — hot path is LS-bound; miss path not the S1 limiter.

---

## Phase 4 — s2s micro-opts (only if s2s-bound)

Already: `orjson`, `memoryview`, upstream batching, connection caps.

**Status:** deferred — baseline saturator is Logstash, not s2s.

---

## Phase 5 — Locked economics

| Item | Locked value |
|------|----------------|
| Planning floor | **0.008 GB/s / stack** |
| Per-stack shape | LS 4 CPU + s2s 2 CPU (6 limit-vCPU) |
| GB/s per limit-vCPU | ~0.0013 |
| Scale path | Horizontal shards: `ceil(ceil(peak/0.008)*1.25)` |

See [sharding.md](sharding.md) and [PERFORMANCE.md](../../PERFORMANCE.md).
