"""Unit tests for classify batch stream dedupe helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from app import (
    ClassifyRequest,
    ClassifyResponse,
    _classify_fields,
    _ensure_unique_streams,
)


def _resp(stream: str) -> ClassifyResponse:
    return ClassifyResponse(
        kind="access_log",
        dataset="access_log",
        namespace="default",
        data_stream=stream,
        pipeline_name="frosty-parse-access-log",
        reason="test",
        fallback=False,
    )


def test_ensure_unique_streams_calls_ensure_once_per_name() -> None:
    manager = MagicMock()
    results = [
        _resp("logs-access_log-default"),
        _resp("logs-access_log-default"),
        _resp("logs-syslog-default"),
    ]
    with patch("app.get_manager", return_value=manager):
        out = _ensure_unique_streams(results)

    assert out == results
    assert manager.ensure_data_stream.call_count == 2
    names = {c.args[0] for c in manager.ensure_data_stream.call_args_list}
    assert names == {"logs-access_log-default", "logs-syslog-default"}


def test_ensure_unique_streams_falls_back_on_failure() -> None:
    manager = MagicMock()

    def ensure(name: str) -> None:
        if name == "logs-access_log-default":
            raise RuntimeError("boom")

    manager.ensure_data_stream.side_effect = ensure
    results = [_resp("logs-access_log-default"), _resp("logs-syslog-default")]

    with patch("app.get_manager", return_value=manager):
        out = _ensure_unique_streams(results)

    assert out[0].fallback is True
    assert out[0].data_stream.startswith("logs-generic-")
    assert out[1].fallback is False
    assert out[1].data_stream == "logs-syslog-default"


def test_classify_fields_works_without_message() -> None:
    result = _classify_fields(
        ClassifyRequest(sourcetype="access_combined", source="", message="", splunk_index="")
    )
    assert result.kind == "access_log"
    assert result.fallback is False


def test_batch_endpoint_dedupes_ensures() -> None:
    from fastapi.testclient import TestClient

    manager = MagicMock()
    with (
        patch("app.ELASTIC_HOST", "https://es.example"),
        patch("app.ELASTIC_API_KEY", "id:secret"),
        patch("app.get_manager", return_value=manager),
    ):
        from app import app

        client = TestClient(app)
        payload = {
            "events": [
                {"sourcetype": "access_combined", "source": "", "message": "ignored"},
                {"sourcetype": "access_combined", "source": "", "message": "also ignored"},
            ]
        }
        resp = client.post("/classify/batch", json=payload)
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["results"]) == 2
        assert manager.ensure_data_stream.call_count == 1
