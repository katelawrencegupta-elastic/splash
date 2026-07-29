# Alerting runbook — Splash

Metrics sources:

| Component | Path | Port |
|-----------|------|------|
| s2s-decode | `/metrics` | 8081 (or shard offset) |
| classify | `/metrics` | 8080 |
| dlq_exporter | `/metrics` | 9102 |
| index_lag_probe | `/metrics` | 9103 |

Rules: [deploy/alerts/splash-alerts.yaml](../../deploy/alerts/splash-alerts.yaml),
recording: [deploy/alerts/splash-recording.yaml](../../deploy/alerts/splash-recording.yaml).

## SplashS2SQueuePegged

**Meaning:** s2s upstream queue &gt; 9000 for 2m (capacity 10000).

**Actions:**
1. Check Logstash CPU/heap and ES bulk latency.
2. Scale out another shard (see [sharding.md](sharding.md)).
3. Confirm classify is not in cold-path storm (`splash:miss_fraction:1m`).
4. If a single poison stream, check DLQ.

## SplashClassifyNotReady

**Meaning:** `splash_pipelines_ready == 0`.

**Actions:**
1. `curl classify:8080/health` — read `reason` / `frosty_pipeline_mode` (no cluster URL is returned).
2. Production (`require`): install `frosty-parse-access-log`, `frosty-parse-syslog`, `frosty-parse-generic` in Elasticsearch.
3. POC only: set `FROSTY_PIPELINE_MODE=stub` and restart classify.
4. Verify ES credentials and network.

## SplashDLQGrowing

**Meaning:** DLQ bytes increased over a 15m window.

**Actions:** Follow [dlq.md](dlq.md) — identify cause, fix ES/frosty/mappings, replay or drop.

## SplashIndexLagHigh

**Meaning:** `splash_index_lag_seconds` &gt; 60 for 10m (index lag probe).

**Actions:**
1. Confirm probe targets and ES `_count` access.
2. Check Logstash bulk latency / queue peg / ES ingest pressure.
3. Temporarily lower offered eps or scale pipeline replicas.

## SplashPeakToAvgHigh

**Meaning:** `splash:peak_to_avg:1d` &gt; 3.

**Actions:**
1. Query `max_over_time(splash:ingest_gbps:5m[1d])` — that is peak GB/s.
2. Re-size: `shards ≈ ceil(ceil(peak_GBps / 0.008) * 1.25)` (see [sharding.md](sharding.md)).
3. Cross-check VIP/NLB ProcessedBytes for the same window.
4. Do not size from daily TB totals alone.

## SplashClassifyMissStorm

**Meaning:** metadata-miss path dominates — `miss_fraction` &gt; 0.25 or miss eps &gt; 2k for 5m while ingest is non-trivial.

**Actions:**
1. Inspect `rate(splash_classify_batch_events_total[5m])` vs `splash:ingest_eps:1m`.
2. Expand [`sidecar/classify_rules.json`](../../sidecar/classify_rules.json) for missing sourcetype/source patterns.
3. Check Splunk UF/HF that `sourcetype` / `source` are populated (empty → cold path).
4. Temporarily scale classify replicas for HA under storm; fix rules before relying on scale.
5. Re-run loadtest `S2` (cold) / `S3` (mixed) after rule changes.

## Scrape + remote_write

Config: [`deploy/prometheus/prometheus.yml`](../../deploy/prometheus/prometheus.yml).

```bash
docker compose --profile metrics up -d --build
```

Prometheus scrapes `s2s-decode:8081`, `classify:8080`, `dlq-exporter:9102`, and `index-lag-probe:9103`, then **remote_writes** to:

`${ELASTIC_HOST}/_prometheus/api/v1/write`

(same cluster as ingest). Override with `PROMETHEUS_REMOTE_WRITE_URL` if needed.

Auth: prefer `PROMETHEUS_ELASTIC_API_KEY` with **metrics-*** privileges; falls back to `ELASTIC_API_KEY` (logs-only keys often get 403 on remote_write). Whitespace-only host/URL vars fail startup.

Multi-shard: `SPLASH_SHARD_ID` becomes `external_labels.splash_shard`; host ports offset via `run-shard.sh` (`9090`/`9102` + stride). UI on loopback `:9090` (shard 0).

```yaml
scrape_configs:
  - job_name: splash-s2s
    static_configs:
      - targets: ["s2s-decode:8081"]
  - job_name: splash-classify
    static_configs:
      - targets: ["classify:8080"]
  - job_name: splash-dlq
    static_configs:
      - targets: ["dlq-exporter:9102"]
  - job_name: splash-index-lag
    static_configs:
      - targets: ["index-lag-probe:9103"]

remote_write:
  - url: ${ELASTIC_HOST}/_prometheus/api/v1/write   # rendered at container start
```

## Security note

mTLS / ingest auth is a separate track (not covered by these alerts).
