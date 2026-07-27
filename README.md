# Splash

Splunk → Elasticsearch ingest bridge. Terminates Splunk forwarder traffic (cooked S2S and uncooked TCP), classifies events into ECS data streams, ensures those streams exist, and indexes into Elastic Cloud as `logs-{dataset}-{namespace}`.

## Architecture

```
Splunk cooked tcpout :39998          Splunk uncooked :39997
         │                                    │
         ▼                                    ▼
  s2s-decode (Python) ──NDJSON──► Logstash :39996 / :39997
                                         │
                           classify_batch.rb (hybrid)
                              │                    │
                    metadata hit            metadata miss
                              │                    │
                    local classify         POST /classify/batch
                    + /ensure/batch              (message path)
                    only if stream new            │
                              │                    │
                              └────────┬───────────┘
                                       ▼
                              splash-classify :8080
                              (policy + ES ensure)
                                       │
                                       ▼
                              Elasticsearch Cloud
```

| Service | Role |
|---------|------|
| **s2s-decode** | Cooked S2S terminator → NDJSON to Logstash `:39996` |
| **logstash** | Uncooked TCP `:39997`, field normalize, hybrid classify, ES output |
| **classify** | Message-pattern classify, `/ensure/batch`, index template + data streams |

### Hybrid classify

Shared rules live in [`sidecar/classify_rules.json`](sidecar/classify_rules.json) (mounted into Logstash).

- **Metadata hit** (`sourcetype` / `source` matches rules): classify in Logstash. Call `POST /ensure/batch` only the first time a data stream is seen; afterward, zero sidecar HTTP for that stream.
- **Metadata miss**: buffer and `POST /classify/batch` (message-pattern / generic path on the sidecar).

Steady-state Splunk traffic with known sourcetype/source pays almost no classify HTTP.

## Quick start

1. Create a `.env` in this directory (or symlink to a parent `.env`). See the example env file next to this repo (`../.env.example` if present) and the variables below.

2. Required:

```bash
ELASTIC_HOST=https://your-cluster.es.region.cloud:443
ELASTIC_API_KEY=id:secret   # or base64 ApiKey
DATA_STREAM_NAMESPACE=default
```

3. Start:

```bash
docker compose up --build -d
```

4. Point Splunk forwarders at this host using [`splunk/outputs.conf`](splunk/outputs.conf):

- Cooked S2S → `:39998` (`s2s-decode`)
- Uncooked plain → `:39997` (`logstash`)

## Ports

| Port | Service | Purpose |
|------|---------|---------|
| 39998 | s2s-decode | Cooked Splunk S2S |
| 39997 | logstash | Uncooked TCP |
| 39996 | logstash (internal) | Decoded NDJSON from s2s-decode |
| 8080 | classify (internal) | Classify / ensure HTTP API |
| 8081 | s2s-decode (internal) | Health + stats |

## Configuration

| Variable | Default | Notes |
|----------|---------|-------|
| `ELASTIC_HOST` | _(required)_ | Elasticsearch / Elastic Cloud URL |
| `ELASTIC_API_KEY` | _(required)_ | ApiKey (`id:secret` or base64) |
| `DATA_STREAM_NAMESPACE` | `default` | ECS namespace segment |
| `CLASSIFY_BATCH_SIZE` | `100` | Message-path batch size |
| `CLASSIFY_FLUSH_MS` | `200` | Message-path flush interval |
| `CLASSIFY_MAX_BUFFER` | `5000` | In-flight classify buffer cap (backpressure) |
| `CLASSIFY_MAX_EGRESS` | `5000` | Classified-but-not-reinjected cap |
| `CLASSIFY_MESSAGE_PREFIX_BYTES` | `512` | Message prefix on metadata-miss path |
| `ELASTIC_HTTP_TIMEOUT_S` | `2.0` | Sidecar → ES HTTP timeout |
| `ELASTIC_ENSURE_CONCURRENCY` | `8` | Max parallel stream ensures per batch |
| `UVICORN_WORKERS` | `1` | Keep `1` for a single `_ensured`/LRU cache |
| `CLASSIFY_HTTP_POOL` | `4` | Logstash keep-alive connections to classify |
| `LS_JAVA_OPTS` | `-Xms1g -Xmx1g` | Logstash heap |
| `S2S_UPSTREAM_*` | see compose | Decoder → Logstash queue / batch |

### Tuning

- Prefer `UVICORN_WORKERS=1` so ensure/classify caches stay in one process.
- Size `CLASSIFY_HTTP_POOL` near Logstash pipeline worker count (default 4).
- Raise `ELASTIC_ENSURE_CONCURRENCY` only if cold multi-stream batches are slow and ES can take the load.

## Sidecar API

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Liveness |
| `POST` | `/classify` | Single-event classify + ensure |
| `POST` | `/classify/batch` | Batch classify + ensure (message path) |
| `POST` | `/ensure/batch` | Ensure data streams only (metadata-local path) |

## Layout

```
splash/
├── docker-compose.yml
├── PERFORMANCE.md          # Perf notes and remaining bottlenecks
├── sidecar/                # FastAPI classify + ES ensure
│   ├── classify_rules.json # Shared metadata rules (canonical)
│   ├── classify.py
│   ├── streams.py
│   └── app.py
├── logstash/               # Pipeline + hybrid Ruby filter
│   ├── pipeline/logstash.conf
│   └── scripts/classify_batch.rb
├── s2s/                    # Cooked S2S decoder
└── splunk/outputs.conf     # Sample forwarder config
```

## Development

Sidecar tests:

```bash
cd sidecar
python3.13 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt pytest
PYTHONPATH=. pytest -q tests/
```

Keep `sidecar/classify_rules.json` and `logstash/scripts/classify_rules.json` in sync (compose overlays the sidecar file at runtime; a unit test asserts the copies match).

## Docs

- [`PERFORMANCE.md`](PERFORMANCE.md) — data-flow, tuning, open bottlenecks
