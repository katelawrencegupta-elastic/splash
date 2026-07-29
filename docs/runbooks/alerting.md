# Alerting runbook — Splash

Metrics sources:

| Component | Path | Port |
|-----------|------|------|
| s2s-decode | `/metrics` | 8081 (or shard offset) |
| classify | `/metrics` | 8080 |
| dlq_exporter | `/metrics` | 9102 |

Rules: [deploy/alerts/splash-alerts.yaml](../../deploy/alerts/splash-alerts.yaml).

## SplashS2SQueuePegged

**Meaning:** s2s upstream queue &gt; 9000 for 2m (capacity 10000).

**Actions:**
1. Check Logstash CPU/heap and ES bulk latency.
2. Scale out another shard (see [sharding.md](sharding.md)).
3. Confirm classify is not in cold-path storm (`/classify/batch` rate).
4. If a single poison stream, check DLQ.

## SplashClassifyNotReady

**Meaning:** `splash_pipelines_ready == 0`.

**Actions:**
1. `curl classify:8080/health` — read reason / `frosty_pipeline_mode`.
2. Production (`require`): install `frosty-parse-access-log`, `frosty-parse-syslog`, `frosty-parse-generic` in Elasticsearch.
3. POC only: set `FROSTY_PIPELINE_MODE=stub` and restart classify.
4. Verify ES credentials and network.

## SplashDLQGrowing

**Meaning:** DLQ bytes increased over a 15m window.

**Actions:** Follow [dlq.md](dlq.md) — identify cause, fix ES/frosty/mappings, replay or drop.

## SplashIndexLagHigh

**Meaning:** External probe reports lag &gt; 60s for 10m.

**Status:** Metric `splash_index_lag_seconds` is optional; export from a probe that compares offered eps to ES `_count` delta. Until deployed, use loadtest summaries and Logstash `:9600/_node/stats` bulk metrics.

## Scrape + remote_write

Config: [`deploy/prometheus/prometheus.yml`](../../deploy/prometheus/prometheus.yml).

```bash
docker compose --profile metrics up -d --build
```

Prometheus scrapes `s2s-decode:8081`, `classify:8080`, and `dlq-exporter:9102`, then **remote_writes** to:

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

remote_write:
  - url: ${ELASTIC_HOST}/_prometheus/api/v1/write   # rendered at container start
```
