# Contract: s2s-decode → Logstash NDJSON

**Owners:** `packages/s2s-decode` produces; `packages/logstash-pipeline` consumes.  
**Version:** 1 (bump when required fields or semantics change).

## Transport

- TCP to Logstash port **39996**
- One JSON object per line (`codec => json_lines` in `logstash.conf`)
- UTF-8; produced with `orjson.dumps(event) + b"\n"`

## Required event fields

Mapped by `s2s.normalize.to_logstash_event`:

| Field | Type | Notes |
|-------|------|--------|
| `host` | string | May be empty |
| `source` | string | May be empty |
| `sourcetype` | string | May be empty |
| `splunk_index` | string | From S2S `index` |
| `message` | string | From `_raw` (or `message`) |
| `tags` | string[] | Default includes `s2s_decoded`, `splunk_tcp_39998` |

## Optional fields

| Field | Type | Notes |
|-------|------|--------|
| `_time` | number or string | Splunk event time when present |
| `s2s.fields` | object | Extra S2S KV not in the known-key set |

## Ownership rules

- **s2s-decode** may change cooked S2S wire framing freely as long as goldens under `testdata/s2s/` pass and this NDJSON shape is preserved.
- Breaking this shape requires a contract version bump and review of Logstash filters (`classify_batch.rb`, `@timestamp` / pipeline mapping in `logstash.conf`).
- Logstash **must not** decode cooked S2S on `:39998`. That port is owned by s2s-decode only. Uncooked plain TCP remains on `:39997`.

## Non-goals

- Shared library across Python and Ruby
- In-process Ruby cooked S2S input inside Logstash
- Authenticating the NDJSON TCP hop (private Docker/K8s network; separate mTLS track)
