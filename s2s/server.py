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

_global_stats = S2SStats()
_upstream_queue: Optional[asyncio.Queue[bytes]] = None
_upstream_task: Optional[asyncio.Task] = None


async def upstream_writer(queue: asyncio.Queue[bytes]) -> None:
    """Maintain a persistent connection to Logstash and write NDJSON lines."""
    while True:
        writer: Optional[asyncio.StreamWriter] = None
        try:
            reader, writer = await asyncio.open_connection(
                LOGSTASH_HOST, LOGSTASH_DECODED_PORT
            )
            logger.info(
                "connected to Logstash %s:%s", LOGSTASH_HOST, LOGSTASH_DECODED_PORT
            )
            while True:
                line = await queue.get()
                writer.write(line)
                await writer.drain()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("upstream writer error: %s; reconnecting in 1s", exc)
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
                try:
                    queue.put_nowait(line)
                except asyncio.QueueFull:
                    logger.error("upstream queue full; dropping event")
            replies = session.take_replies()
            if replies:
                for reply in replies:
                    writer.write(reply)
                await writer.drain()
        for event in session.flush():
            line = (json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8")
            try:
                queue.put_nowait(line)
            except asyncio.QueueFull:
                logger.error("upstream queue full; dropping event on flush")
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
                _global_stats,
                name,
                getattr(_global_stats, name) + getattr(session.stats, name),
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


async def health(_request: web.Request) -> web.Response:
    return web.json_response(
        {
            "status": "ok",
            "stats": {
                "handshake_seen": _global_stats.handshake_seen,
                "frames_ok": _global_stats.frames_ok,
                "frames_bad_magic": _global_stats.frames_bad_magic,
                "frames_bad_kv": _global_stats.frames_bad_kv,
                "frames_oversized": _global_stats.frames_oversized,
                "events_emitted": _global_stats.events_emitted,
                "bytes_consumed": _global_stats.bytes_consumed,
                "capabilities_replied": _global_stats.capabilities_replied,
                "upstream_queue": _upstream_queue.qsize() if _upstream_queue else 0,
            },
        }
    )


async def start_background(app: web.Application) -> None:
    global _upstream_queue, _upstream_task
    _upstream_queue = asyncio.Queue(maxsize=UPSTREAM_QUEUE_SIZE)
    _upstream_task = asyncio.create_task(upstream_writer(_upstream_queue))

    async def _client_cb(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        assert _upstream_queue is not None
        await handle_s2s_client(reader, writer, _upstream_queue)

    server = await asyncio.start_server(
        _client_cb, S2S_LISTEN_HOST, S2S_LISTEN_PORT
    )
    app["s2s_server"] = server
    sockets = ", ".join(str(s.getsockname()) for s in server.sockets or [])
    logger.info("S2S listening on %s", sockets)


async def stop_background(app: web.Application) -> None:
    server: asyncio.AbstractServer = app["s2s_server"]
    server.close()
    await server.wait_closed()
    if _upstream_task is not None:
        _upstream_task.cancel()
        try:
            await _upstream_task
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
