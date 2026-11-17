"""Flusher entrypoint: moves events from the Redis stream into Parquet files. Serves only
GET /health. One flusher per FLUSHER_NAME: a restarted flusher with the same name picks up the
events it had read but not yet written."""

import asyncio
import logging
import os
import signal
import time

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.core.redis import get_redis
from app.modules.ingestion.flusher import Flusher
from app.providers.storage.factory import create_storage

logger = logging.getLogger("app.flusher")

STALE_AFTER_SECONDS = 120


async def serve_health(port: int, last_tick: list[float]) -> asyncio.Server:
    """A minimal HTTP responder, so the same container healthcheck works for API and flusher."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        request_line = await reader.readline()
        ok = request_line.startswith(b"GET /health ") and (
            time.monotonic() - last_tick[0] < STALE_AFTER_SECONDS
        )
        body = b'{"data":{"status":"ok"}}' if ok else b'{"error":{"code":"UNHEALTHY"}}'
        status = b"200 OK" if ok else b"503 Service Unavailable"
        writer.write(
            b"HTTP/1.1 "
            + status
            + b"\r\nContent-Type: application/json\r\nContent-Length: "
            + str(len(body)).encode()
            + b"\r\nConnection: close\r\n\r\n"
            + body
        )
        await writer.drain()
        writer.close()

    return await asyncio.start_server(handle, "0.0.0.0", port)


async def main() -> None:
    configure_logging()
    settings = get_settings()
    flusher = Flusher(
        get_redis(),
        create_storage(settings),
        stream=settings.events_stream,
        consumer=settings.flusher_name,
        max_events=settings.flush_max_events,
        max_seconds=settings.flush_max_seconds,
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    last_tick = [time.monotonic()]
    server = await serve_health(int(os.environ.get("PORT", "8000")), last_tick)
    logger.info("Flusher %s started", settings.flusher_name)
    while not stop.is_set():
        try:
            await flusher.run_once(block_ms=1000)
            last_tick[0] = time.monotonic()
        except Exception:
            logger.exception("Flush failed; events stay in the stream and are retried")
            await asyncio.sleep(1)
    # Write what is buffered before exiting, so a clean shutdown loses nothing.
    await flusher.flush()
    server.close()
    await get_redis().aclose()
    logger.info("Flusher stopped")


if __name__ == "__main__":
    asyncio.run(main())
