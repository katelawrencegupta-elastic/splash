"""Unit tests for async DataStreamManager coalesce behaviour."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from streams import DataStreamManager


@pytest.fixture
async def manager() -> DataStreamManager:
    with patch("streams.httpx.AsyncClient") as client_cls:
        client = MagicMock()
        client.request = AsyncMock()
        client.aclose = AsyncMock()
        client_cls.return_value = client
        mgr = DataStreamManager(
            "https://es.example", "id:secret", frosty_pipeline_mode="stub"
        )
        mgr._client = client
        yield mgr
        await mgr.close()


def _ok_resp() -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"acknowledged": True}
    return resp


@pytest.mark.asyncio
async def test_ensure_data_stream_puts_once_and_caches(manager: DataStreamManager) -> None:
    manager._client.request.side_effect = [_ok_resp(), _ok_resp()]

    await manager.ensure_data_stream("logs-access_log-default")
    await manager.ensure_data_stream("logs-access_log-default")

    assert manager._client.request.await_count == 2
    paths = [c.args[1] for c in manager._client.request.await_args_list]
    assert paths[0].endswith("/_index_template/splash-logs")
    assert paths[1].endswith("/_data_stream/logs-access_log-default")


@pytest.mark.asyncio
async def test_ensure_coalesces_concurrent_puts(manager: DataStreamManager) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    calls: list[str] = []

    async def request(method: str, url: str, json=None):  # noqa: ANN001
        calls.append(url)
        if "/_data_stream/" in url:
            started.set()
            await asyncio.wait_for(release.wait(), timeout=2.0)
        return _ok_resp()

    manager._client.request.side_effect = request

    t1 = asyncio.create_task(manager.ensure_data_stream("logs-syslog-default"))
    await asyncio.wait_for(started.wait(), timeout=2.0)
    t2 = asyncio.create_task(manager.ensure_data_stream("logs-syslog-default"))
    release.set()
    await asyncio.gather(t1, t2)

    stream_puts = [u for u in calls if "/_data_stream/" in u]
    assert len(stream_puts) == 1


@pytest.mark.asyncio
async def test_stream_waiter_outlives_template_plus_stream(
    manager: DataStreamManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Waiters must not abort during cold template+stream (2× HTTP timeout)."""
    import streams as streams_mod

    monkeypatch.setattr(streams_mod, "_ES_TIMEOUT_S", 0.25)
    template_delay = 0.7
    stream_delay = 0.7
    started = asyncio.Event()

    async def request(method: str, url: str, json=None):  # noqa: ANN001
        if "/_index_template/" in url:
            started.set()
            await asyncio.sleep(template_delay)
        elif "/_data_stream/" in url:
            await asyncio.sleep(stream_delay)
        return _ok_resp()

    manager._client.request.side_effect = request

    t1 = asyncio.create_task(manager.ensure_data_stream("logs-slow-default"))
    await asyncio.wait_for(started.wait(), timeout=2.0)
    t2 = asyncio.create_task(manager.ensure_data_stream("logs-slow-default"))
    await asyncio.gather(t1, t2)

    assert "logs-slow-default" in manager._ensured


@pytest.mark.asyncio
async def test_ensure_treats_already_exists_as_success(manager: DataStreamManager) -> None:
    exists = MagicMock()
    exists.status_code = 400
    exists.json.return_value = {
        "error": {"type": "resource_already_exists_exception", "reason": "exists"}
    }
    manager._client.request.side_effect = [_ok_resp(), exists]

    await manager.ensure_data_stream("logs-generic-default")
    assert "logs-generic-default" in manager._ensured


def _missing_pipeline_resp() -> MagicMock:
    resp = MagicMock()
    resp.status_code = 404
    resp.json.return_value = {
        "error": {"type": "resource_not_found_exception", "reason": "missing"}
    }
    return resp


@pytest.mark.asyncio
async def test_ensure_ingest_pipeline_skips_when_present(
    manager: DataStreamManager,
) -> None:
    manager._client.request.side_effect = [_ok_resp()]

    await manager.ensure_ingest_pipeline("frosty-parse-generic")

    assert manager._client.request.await_count == 1
    method, url = manager._client.request.await_args_list[0].args[:2]
    assert method == "GET"
    assert url.endswith("/_ingest/pipeline/frosty-parse-generic")


@pytest.mark.asyncio
async def test_ensure_ingest_pipeline_creates_stub_when_missing(
    manager: DataStreamManager,
) -> None:
    manager._client.request.side_effect = [_missing_pipeline_resp(), _ok_resp()]

    await manager.ensure_ingest_pipeline("frosty-parse-access-log")

    assert manager._client.request.await_count == 2
    methods = [c.args[0] for c in manager._client.request.await_args_list]
    assert methods == ["GET", "PUT"]
    put_kwargs = manager._client.request.await_args_list[1].kwargs
    assert put_kwargs["json"]["processors"] == []


@pytest.mark.asyncio
async def test_ensure_ingest_pipeline_require_mode_fails_when_missing() -> None:
    with patch("streams.httpx.AsyncClient") as client_cls:
        client = MagicMock()
        client.request = AsyncMock(side_effect=[_missing_pipeline_resp()])
        client.aclose = AsyncMock()
        client_cls.return_value = client
        mgr = DataStreamManager(
            "https://es.example", "id:secret", frosty_pipeline_mode="require"
        )
        mgr._client = client
        with pytest.raises(RuntimeError, match="FROSTY_PIPELINE_MODE=require"):
            await mgr.ensure_ingest_pipeline("frosty-parse-generic")
        assert mgr._client.request.await_count == 1
        await mgr.close()


@pytest.mark.asyncio
async def test_ensure_ingest_pipelines_covers_all_kinds(
    manager: DataStreamManager,
) -> None:
    from streams import required_ingest_pipelines

    names = required_ingest_pipelines()
    assert names == [
        "frosty-parse-access-log",
        "frosty-parse-syslog",
        "frosty-parse-generic",
    ]
    # Each name: GET (present) → no PUT; gather may reorder calls
    manager._client.request.side_effect = [_ok_resp()] * len(names)
    ensured = await manager.ensure_ingest_pipelines()
    assert set(ensured) == set(names)
    assert manager._client.request.await_count == len(names)
    called = []
    for c in manager._client.request.await_args_list:
        # httpx mock: args may be (method, url) with absolute or relative path
        url = str(c.args[1] if len(c.args) > 1 else c.kwargs.get("url", ""))
        called.append(url.rsplit("/", 1)[-1])
    assert set(called) == set(names)
