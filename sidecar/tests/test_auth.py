"""Classify HTTP Bearer auth on mutating routes."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def auth_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    import app

    monkeypatch.setattr(app, "CLASSIFY_AUTH_DISABLED", False)
    monkeypatch.setattr(app, "CLASSIFY_AUTH_TOKEN", "test-token")
    monkeypatch.setattr(app, "ELASTIC_HOST", "https://es.example")
    monkeypatch.setattr(app, "ELASTIC_API_KEY", "id:secret")

    manager = MagicMock()
    manager.ensure_data_stream = AsyncMock()
    manager.ensure_template = AsyncMock()
    manager.ensure_ingest_pipelines = AsyncMock(return_value=["frosty-parse-generic"])
    manager.close = AsyncMock()
    monkeypatch.setattr(app, "get_manager", lambda: manager)
    monkeypatch.setattr(app, "_pipelines_ready", True)

    return TestClient(app.app)


def test_ensure_batch_requires_bearer(auth_client: TestClient) -> None:
    resp = auth_client.post(
        "/ensure/batch",
        json={"streams": ["logs-generic-default"]},
    )
    assert resp.status_code == 401


def test_ensure_batch_rejects_bad_token(auth_client: TestClient) -> None:
    resp = auth_client.post(
        "/ensure/batch",
        json={"streams": ["logs-generic-default"]},
        headers={"Authorization": "Bearer wrong"},
    )
    assert resp.status_code == 401


def test_ensure_batch_accepts_bearer(auth_client: TestClient) -> None:
    resp = auth_client.post(
        "/ensure/batch",
        json={"streams": ["logs-generic-default"]},
        headers={"Authorization": "Bearer test-token"},
    )
    assert resp.status_code == 200


def test_health_unauthenticated_and_no_elastic_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app

    monkeypatch.setattr(app, "CLASSIFY_AUTH_DISABLED", False)
    monkeypatch.setattr(app, "CLASSIFY_AUTH_TOKEN", "test-token")
    monkeypatch.setattr(app, "ELASTIC_HOST", "https://secret-es.example:443")
    monkeypatch.setattr(app, "ELASTIC_API_KEY", "id:secret")
    monkeypatch.setattr(app, "_pipelines_ready", True)
    manager = MagicMock()
    manager.close = AsyncMock()
    monkeypatch.setattr(app, "get_manager", lambda: manager)

    client = TestClient(app.app)
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert "elastic_host" not in body
    assert body["status"] == "ok"
    assert body["pipelines_ready"] is True


def test_metrics_unauthenticated(auth_client: TestClient) -> None:
    resp = auth_client.get("/metrics")
    assert resp.status_code == 200
    assert "splash_pipelines_ready" in resp.text


def test_classify_requires_bearer(auth_client: TestClient) -> None:
    resp = auth_client.post(
        "/classify",
        json={"sourcetype": "access_combined", "source": "", "message": ""},
    )
    assert resp.status_code == 401


def test_classify_batch_rb_matches_helm_copy() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    a = (root / "logstash" / "scripts" / "classify_batch.rb").read_text(encoding="utf-8")
    b = (
        root / "deploy" / "helm" / "splash" / "files" / "classify_batch.rb"
    ).read_text(encoding="utf-8")
    assert a == b
