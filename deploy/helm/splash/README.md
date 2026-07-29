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
  --set frostyPipelineMode=require \
  --set pipeline.replicaCount=2 \
  --set classify.replicaCount=2
```

Point Splunk `tcpout` at the LoadBalancer VIP for cooked port 39998
(see chart NOTES after install).

## Capacity

Plan **~0.008–0.009 GB/s (~5k eps) per pipeline replica**. Set
`pipeline.replicaCount` accordingly with ~25% headroom. Use measured **peak**
GB/s (see docs/runbooks/sharding.md), not daily totals alone.

## HA notes

- `classify.replicaCount` defaults to **2** (keep `UVICORN_WORKERS=1` per pod).
  Scale classify for miss-path HA, not raw GB/s.
- Pipeline is a **StatefulSet** with `persistence.enabled=true` (default 5Gi PVC
  per pod) so DLQ / classify_spill survive reschedule.
- mTLS / ingest auth is a separate security track.
