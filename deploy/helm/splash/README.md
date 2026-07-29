# Splash Helm Chart

Minimal Kubernetes deploy: shared **classify** Deployment + **pipeline** Deployment
(s2s-decode + Logstash co-located) behind Services.

## Install

```bash
helm upgrade --install splash ./deploy/helm/splash \
  --namespace splash --create-namespace \
  --set elastic.host="$ELASTIC_HOST" \
  --set elastic.apiKey="$ELASTIC_API_KEY" \
  --set frostyPipelineMode=require \
  --set pipeline.replicaCount=2
```

Point Splunk `tcpout` at the LoadBalancer / Ingress VIP for cooked port 39998
(see chart NOTES after install).

## Capacity

Plan **~0.008–0.009 GB/s (~5k eps) per pipeline replica**. Set
`pipeline.replicaCount` accordingly with ~25% headroom.
