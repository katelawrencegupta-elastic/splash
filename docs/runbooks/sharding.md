# Sharding and VIP — Splash horizontal scale

## Capacity

Load tests (hot cooked path, ~1.5 KB events):

| Unit | Sustainable throughput |
|------|------------------------|
| 1 pipeline stack | **~0.008–0.009 GB/s** (~5k eps) |
| N stacks | **~N × 0.008–0.009 GB/s** |

Planning formula (25% headroom):

```text
shards ≈ ceil(ceil(peak_GBps / 0.008) * 1.25)
```

Use **peak** GB/s, not daily average alone. Example: **1 GB/s peak** → ~125–160 shards. Do not plan above ~0.015 GB/s per stack without a fresh ramp test.

### Measuring peak vs average

Prometheus recording rules ([`deploy/alerts/splash-recording.yaml`](../../deploy/alerts/splash-recording.yaml)):

| Series | Meaning |
|--------|---------|
| `splash:ingest_gbps:5m` | Ingest GB/s from s2s `bytes_consumed` |
| `splash:ingest_eps:1m` | Events/s from s2s `events_emitted` |
| `splash:peak_to_avg:1d` | max(5m GB/s over 1d) / avg(5m GB/s over 1d) |
| `splash_s2s_avg_event_bytes` | Lifetime bytes/event (size skew signal) |

**Workflow:**

1. After ≥24h of production (or a representative load day), query `splash:peak_to_avg:1d` and `max_over_time(splash:ingest_gbps:5m[1d])`.
2. Plug peak into the shard formula above.
3. Cross-check with cloud NLB/VIP **ProcessedBytes** (or equivalent) on the cooked listener — should track s2s bytes within framing overhead.
4. If `splash_s2s_avg_event_bytes` is far from ~1536, re-run the S1 event-size matrix (`S1_512` / `S1_1536` / `S1_4096`) before locking shard count.
5. Alert `SplashPeakToAvgHigh` fires when peak/avg &gt; 3 (daily-total sizing is unsafe).

### Event size

Capacity numbers assume ~1.5 KB events. Smaller events raise CPU per GB; larger raise bytes at fewer filter ops. Measure P50 with `splash_s2s_avg_event_bytes` (or Splunk `_raw` length) and re-measure if ≠ ~1.5 KB.

## Compose shards (already in repo)

```bash
./scripts/run-shard.sh 0 up --build -d   # :39997 / :39998
./scripts/run-shard.sh 1 up --build -d   # :40007 / :40008
```

See [docker-compose.shard.yml](../../docker-compose.shard.yml) and [splunk/outputs.conf](../../splunk/outputs.conf).

## VIP / L4 load balancer

Put a TCP load balancer in front of cooked (and optionally uncooked) ports. Pass-through TCP; do not terminate TLS unless you terminate Splunk separately.

### Backends

- Cooked: `shardN:39998` (or host-mapped ports for single-host multi-shard)
- Uncooked: `shardN:39997`
- Health: TCP connect or HTTP `GET http://shardN:8081/health` (s2s-decode)

### HAProxy example

```text
listen splash_cooked
  bind *:39998
  mode tcp
  balance roundrobin
  option tcp-check
  server s0 10.0.1.10:39998 check
  server s1 10.0.1.11:39998 check
```

### Cloud NLB

Create a TCP Network Load Balancer targeting an ASG / target group of Splash nodes on port 39998. Register s2s health on 8081 if HTTP health checks are supported; otherwise TCP.

Use the NLB byte counters as an independent peak/avg check against Prometheus `splash:ingest_gbps:*`.

### Splunk

Point `tcpout` `server` at the VIP hostname (or multi-server list). Prefer VIP so shard add/remove does not require forwarder config churn.

## Soak checklist

1. Bring up N shards (`run-shard.sh` or Helm).
2. Run loadtest `S1x2`-style at **N × 5k** eps for **≥30 minutes**.
3. Pass if: per-shard `upstream_queue` p99 &lt; 2k, DLQ flat, classify `/health` 200, index lag bounded.
4. Fail if: any shard queue pegged at 10k for &gt;1 minute, or DLQ growing.

## Helm

See [deploy/helm/splash](../../deploy/helm/splash) for Kubernetes multi-replica deploy with a LoadBalancer/ClusterIP Service as the VIP.
