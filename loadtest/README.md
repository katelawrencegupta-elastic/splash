# Splash load-test harness

Synthetic cooked S2S (`:39998`) and uncooked TCP (`:39997`) generators with
health/metrics polling and a JSON summary. Validates the ~5–15k eps capacity model.

## Setup

```bash
cd splash/loadtest
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Start Splash with loopback metrics ports:

```bash
cd ..
# Use an isolated namespace for the run
export DATA_STREAM_NAMESPACE=loadtest
docker compose -f docker-compose.yml -f docker-compose.loadtest.yml up --build -d
```

## Run

```bash
cd loadtest
source .venv/bin/activate

# Hot cooked — 5k eps steady for 2 minutes (plus warm + burst)
python -m loadtest run -s S1 --eps 5000 --duration 120

# Cold path
python -m loadtest run -s S2 --eps 2500 --duration 120

# Mixed 90/10
python -m loadtest run -s S3 --eps 10000 --duration 300

# Uncooked hot
python -m loadtest run -s S4 --eps 5000 --duration 120

# Steady only (smoke)
python -m loadtest run -s S1 --eps 1000 --duration 30 --steady-only -v
```

ES end-to-end ratio (optional): set `ELASTIC_HOST` / `ELASTIC_API_KEY` in the
environment (or pass `--elastic-host` / `--elastic-api-key`). Counts
`logs-*-{namespace}`.

## Scenarios

| ID | Path | Mix |
|----|------|-----|
| S1 | cooked | 100% hot metadata (~1536 B default) |
| S1_512 | cooked | hot, **512 B** event-size matrix |
| S1_1536 | cooked | hot, **1536 B** (planning baseline) |
| S1_4096 | cooked | hot, **4096 B** event-size matrix |
| S2 | cooked | 100% cold (empty sourcetype/source) |
| S3 | cooked | 90% hot / 10% cold |
| S4 | uncooked | 100% hot |

Event-size matrix (re-measure when production P50 ≠ ~1.5 KB):

```bash
python -m loadtest run -s S1_512 --eps 5000 --duration 120
python -m loadtest run -s S1_1536 --eps 5000 --duration 120
python -m loadtest run -s S1_4096 --eps 5000 --duration 120
```

Capacity is primarily **GB/s**-bounded (~0.008/stack). Smaller events raise CPU per
GB; compare steady GB/s and queue peg across the three runs before locking shard
count (see `docs/runbooks/sharding.md`).

Suggest shards from daily volume:

```bash
python suggest_shards.py --tb-day 1 --event-bytes 1536 --peak-factor 2
```

Edit `scenarios.yaml` for defaults (hosts, ports, warm/burst lengths).

## Results

Each run writes `results/<scenario>_<timestamp>/`:

- `settings.yaml` — resolved config
- `metrics_{phase}.csv` — observer samples
- `summary_{phase}.json` — per-phase pass/fail
- `summary.json` — overall (`steady_passed`)

Steady **pass** requires: events sent, upstream queue not pegged near 10k,
classify healthy, and (if ES configured) indexed ratio ≥ 0.95.

## Safety

- Keep `INGEST_BIND=127.0.0.1` unless generators are remote.
- Use `DATA_STREAM_NAMESPACE=loadtest` (or unique) — not production.
- Delete `logs-*-loadtest` data streams after the campaign.
