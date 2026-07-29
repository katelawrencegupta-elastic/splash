# Splash Helm Chart

Minimal Kubernetes deploy: shared **classify** Deployment + **pipeline**
StatefulSet (s2s-decode + Logstash co-located, per-pod PVC for DLQ) behind
Services.

## Install

```bash
helm upgrade --install splash ./deploy/helm/splash \
  --namespace splash --create-namespace \
  --set elastic.host="$ELASTIC_HOST" \
  --set elastic.apiKey="$ELASTIC_API_KEY" \
  --set classify.authToken="$CLASSIFY_AUTH_TOKEN" \
  --set frostyPipelineMode=require \
  --set pipeline.replicaCount=2 \
  --set classify.replicaCount=2
```

`elastic.apiKey` and `classify.authToken` are stored in a chart-managed Secret
(`{{ release }}-splash-credentials`) and mounted via `secretKeyRef` (not plain
env literals). To use an externally managed Secret instead:

```bash
# Secret must contain keys: elastic-api-key, classify-auth-token
--set existingSecret=my-splash-creds
```

Point Splunk `tcpout` at the LoadBalancer VIP for cooked port 39998
(see chart NOTES after install).

## Capacity

Plan **~0.008–0.009 GB/s (~5k eps) per pipeline replica**. Set
`pipeline.replicaCount` accordingly with ~25% headroom. Use measured **peak**
GB/s (see docs/runbooks/sharding.md), not daily totals alone.

CPU requests/limits (defaults):

| Container | Requests | Limits |
|-----------|----------|--------|
| classify | 100m / 256Mi | 1 / 512Mi |
| s2s-decode | 200m / 256Mi | 2 / 1Gi |
| logstash | 500m / 1536Mi | 4 / 2Gi |

`pipeline.workers` defaults to **4** (matches Logstash CPU limit).
Pods use `terminationGracePeriodSeconds: 40` (covers s2s 30s drain).

## HA / security notes

- `classify.replicaCount` defaults to **2** (keep `UVICORN_WORKERS=1` per pod).
  Scale classify for miss-path HA, not raw GB/s.
- Pipeline is a **StatefulSet** with `persistence.enabled=true` (default 5Gi PVC
  per pod) so DLQ / classify_spill survive reschedule.
- Classify HTTP middleware requires `Authorization: Bearer …` on mutating
  routes (`/classify*`, `/ensure/*`). `/health` and `/metrics` stay open for
  probes and scrapes. mTLS / Splunk ingest auth remains a separate track.
- Readiness + liveness probes are set on classify, Logstash, s2s-decode,
  dlq-exporter, and index-lag-probe.
