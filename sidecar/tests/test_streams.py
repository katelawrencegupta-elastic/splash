"""Unit tests for DataStreamManager lock / coalesce behaviour."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

from streams import DataStreamManager


@pytest.fixture
def manager() -> DataStreamManager:
    with patch("streams.httpx.Client") as client_cls:
        client = MagicMock()
        client_cls.return_value = client
        mgr = DataStreamManager("https://es.example", "id:secret")
        mgr._client = client
        yield mgr
        mgr.close()


def test_ensure_data_stream_puts_once_and_caches(manager: DataStreamManager) -> None:
    manager._client.request.side_effect = [
        MagicMock(status_code=200, json=lambda: {"acknowledged": True}),  # template
        MagicMock(status_code=200, json=lambda: {"acknowledged": True}),  # stream
    ]

    manager.ensure_data_stream("logs-access_log-default")
    manager.ensure_data_stream("logs-access_log-default")

    assert manager._client.request.call_count == 2
    paths = [c.args[1] for c in manager._client.request.call_args_list]
    assert paths[0].endswith("/_index_template/splash-logs")
    assert paths[1].endswith("/_data_stream/logs-access_log-default")


def test_ensure_coalesces_concurrent_puts(manager: DataStreamManager) -> None:
    started = threading.Event()
    release = threading.Event()
    calls: list[str] = []

    def request(method: str, url: str, json=None):  # noqa: ANN001
        calls.append(url)
        if "/_data_stream/" in url:
            started.set()
            assert release.wait(timeout=2.0)
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"acknowledged": True}
        return resp

    manager._client.request.side_effect = request

    errors: list[BaseException] = []

    def worker() -> None:
        try:
            manager.ensure_data_stream("logs-syslog-default")
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    assert started.wait(timeout=2.0)
    t2.start()
    release.set()
    t1.join(timeout=2.0)
    t2.join(timeout=2.0)

    assert errors == []
    stream_puts = [u for u in calls if "/_data_stream/" in u]
    assert len(stream_puts) == 1


def test_ensure_treats_already_exists_as_success(manager: DataStreamManager) -> None:
    manager._client.request.side_effect = [
        MagicMock(status_code=200, json=lambda: {"acknowledged": True}),
        MagicMock(
            status_code=400,
            json=lambda: {
                "error": {"type": "resource_already_exists_exception", "reason": "exists"}
            },
        ),
    ]

    manager.ensure_data_stream("logs-generic-default")
    assert "logs-generic-default" in manager._ensured
