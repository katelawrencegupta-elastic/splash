"""Tests for POST /ensure/batch and classify rules loading."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from classify import classify_event, load_classify_rules


def test_load_classify_rules_has_required_keys() -> None:
    rules = load_classify_rules()
    assert "access_sourcetype" in rules
    assert "{kind}" in rules["pipeline_name_template"]


def test_logstash_rules_file_matches_sidecar() -> None:
    sidecar = Path(__file__).resolve().parents[1] / "classify_rules.json"
    logstash = (
        Path(__file__).resolve().parents[2]
        / "logstash"
        / "scripts"
        / "classify_rules.json"
    )
    assert sidecar.is_file()
    assert logstash.is_file()
    assert sidecar.read_text(encoding="utf-8") == logstash.read_text(encoding="utf-8")


def test_metadata_parity_table() -> None:
    cases = [
        ("access_combined", "", "", "access_log"),
        ("syslog", "", "", "syslog"),
        ("", "/var/log/nginx/access.log", "", "access_log"),
        ("", "/var/log/syslog", "", "syslog"),
        ("sourcetype::nginx:access", "", "web", "access_log"),
    ]
    for sourcetype, source, index, kind in cases:
        result = classify_event(
            sourcetype=sourcetype, source=source, message="", splunk_index=index
        )
        assert result.kind.value == kind, (sourcetype, source, index)


def test_ensure_batch_endpoint() -> None:
    manager = MagicMock()

    with (
        patch("app.ELASTIC_HOST", "https://es.example"),
        patch("app.ELASTIC_API_KEY", "id:secret"),
        patch("app.DATA_STREAM_NAMESPACE", "default"),
        patch("app.get_manager", return_value=manager),
    ):
        from app import app

        client = TestClient(app)
        resp = client.post(
            "/ensure/batch",
            json={
                "streams": [
                    "logs-access_log-default",
                    "logs-access_log-default",
                    "logs-syslog-default",
                ]
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["results"]) == 2
        assert body["results"][0]["ok"] is True
        assert body["results"][0]["resolved_stream"] == "logs-access_log-default"
        assert manager.ensure_data_stream.call_count == 2


def test_ensure_batch_fallback_on_failure() -> None:
    manager = MagicMock()

    def ensure(name: str) -> None:
        if name == "logs-access_log-default":
            raise RuntimeError("boom")

    manager.ensure_data_stream.side_effect = ensure

    with (
        patch("app.ELASTIC_HOST", "https://es.example"),
        patch("app.ELASTIC_API_KEY", "id:secret"),
        patch("app.DATA_STREAM_NAMESPACE", "default"),
        patch("app.get_manager", return_value=manager),
    ):
        from app import app

        client = TestClient(app)
        resp = client.post(
            "/ensure/batch",
            json={"streams": ["logs-access_log-default"]},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["results"][0]["ok"] is False
        assert body["results"][0]["fallback"] is True
        assert body["results"][0]["resolved_stream"] == "logs-generic-default"
