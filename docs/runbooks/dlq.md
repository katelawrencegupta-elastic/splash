# Dead letter queue (DLQ) runbook — Splash Logstash

## Where it lives

Logstash is configured with:

```yaml
# packages/logstash-pipeline/config/logstash.yml
dead_letter_queue.enable: true
dead_letter_queue.max_bytes: 1073741824
```

On disk (compose volume `logstash_data`):

```text
/usr/share/logstash/data/dead_letter_queue/
```

Inspect from the host:

```bash
docker compose exec logstash ls -la /usr/share/logstash/data/dead_letter_queue
# or for a shard:
docker compose -p splash0 exec logstash du -sh /usr/share/logstash/data/dead_letter_queue
```

Prometheus gauge (if dlq_exporter is running): `splash_dlq_files`, `splash_dlq_bytes_total`.

## How to detect growth

1. Alert `SplashDLQGrowing` (see [alerting.md](alerting.md)) — bytes increasing over 15m.
2. Manual: `du -sh` on the DLQ path; compare over time.
3. Correlate with ES bulk errors in Logstash logs and Elastic Cloud ingest metrics.

## When to replay vs drop

| Cause | Action |
|-------|--------|
| Transient ES outage / 429 / timeout | Replay after ES is healthy |
| Missing frosty ingest pipeline | Fix pipelines (`FROSTY_PIPELINE_MODE=require`), then replay |
| Mapping / field conflict | Fix template/mappings; may need to drop poison events |
| Malformed document | Drop or fix offline; do not infinite-replay |

## Replay procedure (outline)

Logstash can consume its own DLQ via a dedicated pipeline. Example input (run as a **one-shot** or temporary pipeline — do not leave enabled permanently alongside the main pipeline without isolating indices):

```ruby
input {
  dead_letter_queue {
    path => "/usr/share/logstash/data/dead_letter_queue"
    commit_offsets => true
  }
}
output {
  elasticsearch {
    hosts => ["${ELASTIC_HOST}"]
    api_key => "${ELASTIC_API_KEY}"
    ssl_enabled => true
    index => "%{[@metadata][target_stream]}"
    pipeline => "%{[@metadata][pipeline]}"
    action => "create"
    manage_template => false
  }
}
```

Safer offline approach:

1. Copy DLQ segments out of the volume to a scratch directory.
2. Replay with a temporary Logstash container mounting that copy.
3. Confirm indexed counts, then delete replayed segments.

## After recovery

- Confirm `splash_dlq_bytes_total` is flat or falling.
- Confirm s2s `upstream_queue` is not pegged.
- Note root cause in the incident log (ES capacity, frosty missing, mapping).
