# Shared S2S golden fixtures (Python + Ruby)

Binary corpora and `manifest.json` exercise both:

- `splash/s2s` pytest (`tests/test_golden.py`) — production `s2s-decode` path
- `splash/logstash/plugins/logstash-input-s2s/test_decoder.rb` — Ruby port

**Any protocol / framing fix must update fixtures here and pass both suites.**

Regenerate bins (from `splash/`):

```bash
# Prefer editing the Python generators in s2s/s2s/testdata then re-running
# the small write script used in CI/dev, or extend test_golden helpers.
cd s2s && pytest tests/test_golden.py
ruby ../logstash/plugins/logstash-input-s2s/test_decoder.rb
```

Stats names (canonical): `frames_ok`, `frames_bad_magic`, `frames_bad_kv`,
`frames_oversized`. KV body parse failures are reported as errors prefixed
`kv:` so both decoders can classify them.
