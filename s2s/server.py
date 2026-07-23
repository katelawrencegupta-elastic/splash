"""TCP terminator: accept cooked S2S, emit NDJSON to Logstash."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Optional

from aiohttp import web

from s2s.decoder import S2SSession, S2SStats
from s2s.framing import DEFAULT_MAX_FRAME_SIZE

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("s2s.server")

S2S_LISTEN_HOST = os.environ.get("S2S_LISTEN_HOST", "0.0.0.0")
S2S_LISTEN_PORT = int(os.environ.get("S2S_LISTEN_PORT", "39998"))
LOGSTASH_HOST = os.environ.get("LOGSTASH_HOST", "logstash")
LOGSTASH_DECODED_PORT = int(os.environ.get("LOGSTASH_DECODED_PORT", "39996"))
HEALTH_PORT = int(os.environ.get("S2S_HEALTH_PORT", "8081"))
MAX_FRAME_SIZE = int(os.environ.get("S2S_MAX_FRAME_SIZE", str(DEFAULT_MAX_FRAME_SIZE)))
UPSTREAM_QUEUE_SIZE = int(os.environ.get("S2S_UPSTREAM_QUEUE_SIZE", "10000"))
UPSTREAM_BATCH_SIZE = int(os.environ.get("S2S_UPSTREAM_BATCH_SIZE", "100"))
UPSTREAM_FLUSH_MS = int(os.environ.get("S2S_UPSTREAM_FLUSH_MS", "50"))


async def _fill_batch(
    queue: asyncio.Queue[bytes],
    first: bytes,
    *,
    batch_size: int,
    flush_ms: int,
) -> list[bytes]:
    """Collect up to batch_size items, waiting at most flush_ms for more."""
    batch = [first]
    if batch_size <= 1 or flush_ms <= 0:
        # Opportunistic non-blocking fill when flush is disabled
        while len(batch) < batch_size:
            try:
                batch.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return batch

    deadline = time.monotonic() + (flush_ms / 1000.0)
    while len(batch) < batch_size:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            item = await asyncio.wait_for(queue.get(), timeout=remaining)
        except asyncio.TimeoutError:
            break
        batch.append(item)
    return batch


async def upstream_writer(
    queue: asyncio.Queue[bytes],
    *,
    batch_size: int = UPSTREAM_BATCH_SIZE,
    flush_ms: int = UPSTREAM_FLUSH_MS,
) -> None:
    """Maintain a persistent connection to Logstash and write batched NDJSON.

    Items already dequeued stay in ``inflight`` until ``drain()`` succeeds, so a
    reconnect does not silently drop the current batch.
    """
    inflight: list[bytes] = []
    batch_size = max(1, batch_size)

    while True:
        writer: Optional[asyncio.StreamWriter] = None
        try:
            _reader, writer = await asyncio.open_connection(
                LOGSTASH_HOST, LOGSTASH_DECODED_PORT
            )
            logger.info(
                "connected to Logstash %s:%s", LOGSTASH_HOST, LOGSTASH_DECODED_PORT
            )
            while True:
                if not inflight:
                    first = await queue.get()
                    inflight = await _fill_batch(
                        queue,
                        first,
                        batch_size=batch_size,
                        flush_ms=flush_ms,
                    )

                for line in inflight:
                    writer.write(line)
                await writer.drain()
                inflight = []
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "upstream writer error: %s; retrying %s in-flight line(s) after 1s",
                exc,
                len(inflight),
            )
            await asyncio.sleep(1.0)
        finally:
            if writer is not None:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass


async def handle_s2s_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    queue: asyncio.Queue[bytes],
    stats: S2SStats,
) -> None:
    peer = writer.get_extra_info("peername")
    session = S2SSession(max_frame_size=MAX_FRAME_SIZE)
    logger.info("S2S connection from %s", peer)
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            for event in session.feed(data):
                line = (json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8")
                # Block when full so Splunk TCP window applies backpressure
                await queue.put(line)
            replies = session.take_replies()
            if replies:
                for reply in replies:
                    writer.write(reply)
                await writer.drain()
        for event in session.flush():
            line = (json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8")
            await queue.put(line)
        replies = session.take_replies()
        if replies:
            for reply in replies:
                writer.write(reply)
            await writer.drain()
    except Exception as exc:
        logger.exception("client handler error from %s: %s", peer, exc)
    finally:
        for name in (
            "handshake_seen",
            "frames_ok",
            "frames_bad_magic",
            "frames_bad_kv",
            "frames_oversized",
            "events_emitted",
            "bytes_consumed",
            "capabilities_replied",
        ):
            setattr(
                stats,
                name,
                getattr(stats, name) + getattr(session.stats, name),
            )
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        logger.info(
            "S2S connection closed %s frames_ok=%s events=%s caps_replied=%s",
            peer,
            session.stats.frames_ok,
            session.stats.events_emitted,
            session.stats.capabilities_replied,
        )


async def health(request: web.Request) -> web.Response:
    stats: S2SStats = request.app["stats"]
    queue: asyncio.Queue[bytes] = request.app["upstream_queue"]
    return web.json_response(
        {
            "status": "ok",
            "stats": {
                "handshake_seen": stats.handshake_seen,
                "frames_ok": stats.frames_ok,
                "frames_bad_magic": stats.frames_bad_magic,
                "frames_bad_kv": stats.frames_bad_kv,
                "frames_oversized": stats.frames_oversized,
                "events_emitted": stats.events_emitted,
                "bytes_consumed": stats.bytes_consumed,
                "capabilities_replied": stats.capabilities_replied,
                "upstream_queue": queue.qsize(),
                "upstream_batch_size": UPSTREAM_BATCH_SIZE,
                "upstream_flush_ms": UPSTREAM_FLUSH_MS,
            },
        }
    )


async def start_background(app: web.Application) -> None:
    queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=UPSTREAM_QUEUE_SIZE)
    stats = S2SStats()
    task = asyncio.create_task(
        upstream_writer(
            queue,
            batch_size=UPSTREAM_BATCH_SIZE,
            flush_ms=UPSTREAM_FLUSH_MS,
        )
    )
    app["upstream_queue"] = queue
    app["upstream_task"] = task
    app["stats"] = stats

    async def _client_cb(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await handle_s2s_client(reader, writer, queue, stats)

    server = await asyncio.start_server(
        _client_cb, S2S_LISTEN_HOST, S2S_LISTEN_PORT
    )
    app["s2s_server"] = server
    sockets = ", ".join(str(s.getsockname()) for s in server.sockets or [])
    logger.info(
        "S2S listening on %s (upstream batch_size=%s flush_ms=%s queue=%s)",
        sockets,
        UPSTREAM_BATCH_SIZE,
        UPSTREAM_FLUSH_MS,
        UPSTREAM_QUEUE_SIZE,
    )


async def stop_background(app: web.Application) -> None:
    server: asyncio.AbstractServer = app["s2s_server"]
    server.close()
    await server.wait_closed()
    task: asyncio.Task = app["upstream_task"]
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def main() -> None:
    app = web.Application()
    app.router.add_get("/health", health)
    app.on_startup.append(start_background)
    app.on_cleanup.append(stop_background)
    web.run_app(app, host="0.0.0.0", port=HEALTH_PORT, print=None)


if __name__ == "__main__":
    main()
