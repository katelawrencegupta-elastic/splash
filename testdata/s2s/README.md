# Shared S2S golden fixtures

Binary corpora and `manifest.json` exercise the production Python
`s2s-decode` path (`packages/s2s-decode` pytest `tests/test_golden.py`).

Cooked ingest is Python-only. Protocol / framing fixes must update fixtures
here and pass the golden suite.

Regenerate / verify (from `splash/`):

```bash
cd packages/s2s-decode && PYTHONPATH=. pytest tests/test_golden.py
```

Prefer editing the Python generators in `packages/s2s-decode/s2s/testdata`
then extending golden helpers as needed.

Stats names (canonical): `frames_ok`, `frames_bad_magic`, `frames_bad_kv`,
`frames_oversized`. KV body parse failures are reported as errors prefixed
`kv:` so the decoder can classify them.

Handoff to Logstash after decode is documented in
[`docs/contracts/s2s-ndjson.md`](../../docs/contracts/s2s-ndjson.md).
