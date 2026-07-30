"""Cooked S2S load generator → s2s-decode :39998."""

from __future__ import annotations

import asyncio
import logging
import random
import sys
from pathlib import Path

# Import the in-repo s2s package (packages/s2s-decode).
_S2S_ROOT = Path(__file__).resolve().parents[2] / "packages" / "s2s-decode"
if str(_S2S_ROOT) not in sys.path:
    sys.path.insert(0, str(_S2S_ROOT))

from s2s.testdata import make_capabilities_message, make_event_frame, make_handshake  # noqa: E402

from .payloads import fields_for
from .rate import RateLimiter
from .stats import GenStats

logger = logging.getLogger("loadtest.cooked")


async def _drain_replies(reader: asyncio.StreamReader, timeout: float = 0.2) -> None:
    """Read capability replies without blocking the send loop for long."""
    try:
        await asyncio.wait_for(reader.read(65536), timeout=timeout)
    except (asyncio.TimeoutError, ConnectionError, asyncio.IncompleteReadError):
        return


async def _one_connection(
    *,
    host: str,
    port: int,
    limiter: RateLimiter,
    stats: GenStats,
    stop: asyncio.Event,
    hot_fraction: float,
    event_bytes: int,
    seq_start: int,
    worker_id: int,
) -> None:
    seq = seq_start
    while not stop.is_set():
        try:
            reader, writer = await asyncio.open_connection(host, port)
        except OSError as exc:
            stats.errors += 1
            logger.warning("worker=%s connect failed: %s", worker_id, exc)
            await asyncio.sleep(0.5)
            continue

        stats.connects += 1
        try:
            writer.write(make_handshake(version=3))
            writer.write(make_capabilities_message())
            await writer.drain()
            await _drain_replies(reader)

            while not stop.is_set():
                await limiter.acquire()
                hot = random.random() < hot_fraction
                frame = make_event_frame(
                    fields_for(hot=hot, seq=seq, event_bytes=event_bytes)
                )
                seq += 1
                writer.write(frame)
                if seq % 32 == 0:
                    await writer.drain()
                    if reader.at_eof():
                        break
                stats.sent_events += 1
                stats.sent_bytes += len(frame)
        except (ConnectionError, OSError, asyncio.IncompleteReadError) as exc:
            stats.errors += 1
            logger.warning("worker=%s connection error: %s", worker_id, exc)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            await asyncio.sleep(0.05)


async def run_cooked(
    *,
    host: str,
    port: int,
    eps: float,
    duration_s: float,
    connections: int,
    hot_fraction: float,
    event_bytes: int,
    stats: GenStats | None = None,
    stop: asyncio.Event | None = None,
) -> GenStats:
    stats = stats or GenStats()
    stop = stop or asyncio.Event()
    limiter = RateLimiter(eps, burst=max(eps / 10.0, 1.0))
    workers = [
        asyncio.create_task(
            _one_connection(
                host=host,
                port=port,
                limiter=limiter,
                stats=stats,
                stop=stop,
                hot_fraction=hot_fraction,
                event_bytes=event_bytes,
                seq_start=i * 1_000_000_000,
                worker_id=i,
            ),
            name=f"cooked-{i}",
        )
        for i in range(max(1, connections))
    ]
    try:
        await asyncio.sleep(duration_s)
    finally:
        stop.set()
        await asyncio.gather(*workers, return_exceptions=True)
    return stats
