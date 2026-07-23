"""Tests for upstream writer reliability (retry + batching + backpressure)."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server import _fill_batch, upstream_writer  # noqa: E402


def test_fill_batch_respects_size_and_flush():
    async def _run() -> None:
        queue: asyncio.Queue[bytes] = asyncio.Queue()
        for i in range(5):
            queue.put_nowait(f"line-{i}\n".encode())

        first = await queue.get()
        batch = await _fill_batch(queue, first, batch_size=3, flush_ms=50)
        assert len(batch) == 3
        assert batch[0] == b"line-0\n"
        assert queue.qsize() == 2

    asyncio.run(_run())


def test_upstream_writer_retries_inflight_after_disconnect(monkeypatch):
    """An item dequeued before a failed write must be resent after reconnect."""

    async def _run() -> None:
        queue: asyncio.Queue[bytes] = asyncio.Queue()
        await queue.put(b"event-1\n")
        await queue.put(b"event-2\n")

        writes: list[bytes] = []
        connect_attempts = {"n": 0}

        class FakeWriter:
            def __init__(self, fail_first: bool):
                self.fail_first = fail_first

            def write(self, data: bytes) -> None:
                if self.fail_first:
                    raise ConnectionResetError("boom")
                writes.append(data)

            async def drain(self) -> None:
                return None

            def close(self) -> None:
                return None

            async def wait_closed(self) -> None:
                return None

        async def fake_open_connection(host, port):
            connect_attempts["n"] += 1
            fail = connect_attempts["n"] == 1
            return None, FakeWriter(fail_first=fail)

        real_sleep = asyncio.sleep

        async def fake_sleep(_delay: float) -> None:
            # Yield to the event loop without waiting a full second on reconnect.
            await real_sleep(0)

        monkeypatch.setattr("server.asyncio.open_connection", fake_open_connection)
        monkeypatch.setattr("server.asyncio.sleep", fake_sleep)

        task = asyncio.create_task(upstream_writer(queue, batch_size=1, flush_ms=0))
        try:
            for _ in range(200):
                if writes == [b"event-1\n", b"event-2\n"]:
                    break
                await real_sleep(0)
            assert writes == [b"event-1\n", b"event-2\n"]
            assert connect_attempts["n"] >= 2
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(_run())


def test_upstream_writer_batches_before_drain(monkeypatch):
    async def _run() -> None:
        queue: asyncio.Queue[bytes] = asyncio.Queue()
        for i in range(4):
            await queue.put(f"e{i}\n".encode())

        drain_counts: list[int] = []
        buffer: list[bytes] = []

        class FakeWriter:
            def write(self, data: bytes) -> None:
                buffer.append(data)

            async def drain(self) -> None:
                drain_counts.append(len(buffer))
                buffer.clear()

            def close(self) -> None:
                return None

            async def wait_closed(self) -> None:
                return None

        async def fake_open_connection(host, port):
            return None, FakeWriter()

        monkeypatch.setattr("server.asyncio.open_connection", fake_open_connection)

        task = asyncio.create_task(
            upstream_writer(queue, batch_size=4, flush_ms=200)
        )
        try:
            for _ in range(50):
                if drain_counts:
                    break
                await asyncio.sleep(0.01)
            assert drain_counts[0] == 4
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(_run())


def test_queue_put_applies_backpressure():
    async def _run() -> None:
        queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1)
        await queue.put(b"full\n")

        put_task = asyncio.create_task(queue.put(b"blocked\n"))
        await asyncio.sleep(0.02)
        assert not put_task.done()

        assert await queue.get() == b"full\n"
        await asyncio.wait_for(put_task, timeout=1.0)
        assert queue.qsize() == 1

    asyncio.run(_run())
