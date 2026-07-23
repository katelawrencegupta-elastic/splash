"""TCP terminator: accept cooked S2S, emit NDJSON to Logstash."""

from __future__ import annotations

import asyncio
import json
import logging
import os
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
UPSTREAM_DRAIN_TIMEOUT_S = float(os.environ.get("S2S_UPSTREAM_DRAIN_TIMEOUT_S", "30"))

# Queue sentinel: stop accepting work and let the writer exit after drain.
_SHUTDOWN = object()


async def _fill_batch(
    queue: asyncio.Queue,
    first: bytes,
    *,
    batch_size: int,
    flush_ms: int,
) -> list[bytes] | object:
    """Collect up to batch_size items without per-item wait_for tasks.

    Drains immediately available items, then sleeps once up to ``flush_ms`` and
    drains again. Returns ``_SHUTDOWN`` if the shutdown sentinel is seen.
    """
    if first is _SHUTDOWN:
        return _SHUTDOWN

    batch: list[bytes] = [first]
    if batch_size <= 1:
        return batch

    while len(batch) < batch_size:
        try:
            item = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        if item is _SHUTDOWN:
            # Preserve sentinel for the writer loop; return what we have.
            await queue.put(_SHUTDOWN)
            return batch

        batch.append(item)

    if len(batch) >= batch_size or flush_ms <= 0:
        return batch

    await asyncio.sleep(flush_ms / 1000.0)

    while len(batch) < batch_size:
        try:
            item = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        if item is _SHUTDOWN:
            await queue.put(_SHUTDOWN)
            return batch
        batch.append(item)
    return batch


async def _write_batch(writer: asyncio.StreamWriter, batch: list[bytes]) -> None:
    for line in batch:
        writer.write(line)
    await writer.drain()


async def _close_upstream(
    reader: Optional[asyncio.StreamReader],
    writer: Optional[asyncio.StreamWriter],
    peer_watch: Optional[asyncio.Task],
) -> None:
    if peer_watch is not None and not peer_watch.done():
        peer_watch.cancel()
        try:
            await peer_watch
        except asyncio.CancelledError:
            pass
    if writer is not None:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
    # reader shares the transport with writer; closing writer is enough.
    _ = reader


async def upstream_writer(
    queue: asyncio.Queue,
    *,
    batch_size: int = UPSTREAM_BATCH_SIZE,
    flush_ms: int = UPSTREAM_FLUSH_MS,
) -> None:
    """Maintain a persistent connection to Logstash and write batched NDJSON.

    Items already dequeued stay in ``inflight`` until ``drain()`` succeeds, so a
    reconnect does not silently drop the current batch. A ``_SHUTDOWN`` sentinel
    drains inflight then exits cleanly.
    """
    inflight: list[bytes] = []
    batch_size = max(1, batch_size)

    while True:
        reader: Optional[asyncio.StreamReader] = None
        writer: Optional[asyncio.StreamWriter] = None
        peer_watch: Optional[asyncio.Task] = None
        try:
            reader, writer = await asyncio.open_connection(
                LOGSTASH_HOST, LOGSTASH_DECODED_PORT
            )
            # Detect Logstash closing the socket so we reconnect promptly.
            peer_watch = asyncio.create_task(
                reader.read(1), name="logstash-peer-watch"
            )
            logger.info(
                "connected to Logstash %s:%s", LOGSTASH_HOST, LOGSTASH_DECODED_PORT
            )
            while True:
                if peer_watch.done():
                    raise ConnectionResetError("Logstash closed upstream connection")

                if not inflight:
                    first = await queue.get()
                    if first is _SHUTDOWN:
                        return
                    filled = await _fill_batch(
                        queue,
                        first,
                        batch_size=batch_size,
                        flush_ms=flush_ms,
                    )
                    if filled is _SHUTDOWN:
                        return
                    inflight = filled  # type: ignore[assignment]

                await _write_batch(writer, inflight)
                inflight = []
        except asyncio.CancelledError:
            if writer is not None and inflight:
                try:
                    await _write_batch(writer, inflight)
                    inflight = []
                except Exception as exc:
                    logger.warning(
                        "final drain of %s in-flight line(s) failed: %s",
                        len(inflight),
                        exc,
                    )
            raise
        except Exception as exc:
            # Exiting on shutdown sentinel path uses return, not exception.
            if inflight and isinstance(exc, ConnectionResetError):
                logger.warning(
                    "upstream writer error: %s; retrying %s in-flight line(s) after 1s",
                    exc,
                    len(inflight),
                )
            else:
                logger.warning(
                    "upstream writer error: %s; retrying %s in-flight line(s) after 1s",
                    exc,
                    len(inflight),
                )
            await asyncio.sleep(1.0)
        finally:
            await _close_upstream(reader, writer, peer_watch)


async def handle_s2s_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    queue: asyncio.Queue,
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
    queue: asyncio.Queue = request.app["upstream_queue"]
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
    queue: asyncio.Queue = asyncio.Queue(maxsize=UPSTREAM_QUEUE_SIZE)
    stats = S2SStats()
    task = asyncio.create_task(
        upstream_writer(
            queue,
            batch_size=UPSTREAM_BATCH_SIZE,
            flush_ms=UPSTREAM_FLUSH_MS,
        ),
        name="upstream-writer",
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
    """Stop accepting clients, drain the upstream queue, then stop the writer."""
    server: asyncio.AbstractServer = app["s2s_server"]
    server.close()
    await server.wait_closed()
    logger.info("S2S listener closed; draining upstream queue")

    queue: asyncio.Queue = app["upstream_queue"]
    task: asyncio.Task = app["upstream_task"]
    try:
        await asyncio.wait_for(queue.put(_SHUTDOWN), timeout=5.0)
    except asyncio.TimeoutError:
        logger.warning("could not enqueue shutdown sentinel; cancelling writer")
        task.cancel()

    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=UPSTREAM_DRAIN_TIMEOUT_S)
        logger.info("upstream writer drained and exited")
    except asyncio.TimeoutError:
        logger.warning(
            "upstream drain timed out after %ss with ~%s queued; cancelling",
            UPSTREAM_DRAIN_TIMEOUT_S,
            queue.qsize(),
        )
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
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
