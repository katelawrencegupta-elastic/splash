# Sharding and VIP — Splash horizontal scale

## Capacity

Load tests (hot cooked path, ~1.5 KB events):

| Unit | Sustainable throughput |
|------|------------------------|
| 1 pipeline stack | **~0.008–0.009 GB/s** (~5k eps) |
| N stacks | **~N × 0.008–0.009 GB/s** |

Planning formula (25% headroom):

```text
shards ≈ ceil(target_GBps / 0.008) * 1.25
```

Example: **1 GB/s** → ~125–160 shards. Do not plan above ~0.015 GB/s per stack without a fresh ramp test.

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

### Splunk

Point `tcpout` `server` at the VIP hostname (or multi-server list). Prefer VIP so shard add/remove does not require forwarder config churn.

## Soak checklist

1. Bring up N shards (`run-shard.sh` or Helm).
2. Run loadtest `S1x2`-style at **N × 5k** eps for **≥30 minutes**.
3. Pass if: per-shard `upstream_queue` p99 &lt; 2k, DLQ flat, classify `/health` 200, index lag bounded.
4. Fail if: any shard queue pegged at 10k for &gt;1 minute, or DLQ growing.

## Helm

See [deploy/helm/splash](../../deploy/helm/splash) for Kubernetes multi-replica deploy with a LoadBalancer/ClusterIP Service as the VIP.
