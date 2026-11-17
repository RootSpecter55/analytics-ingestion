import asyncio
import json
import logging
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.core.lake import partition_prefix, to_parquet
from app.providers.storage.base import StorageProvider

logger = logging.getLogger(__name__)

GROUP = "flusher"


def to_row(event: dict[str, Any]) -> dict[str, Any]:
    """A stream entry as a Parquet row (properties stay JSON text)."""
    return {
        **event,
        "timestamp": datetime.fromisoformat(event["timestamp"]),
        "received_at": datetime.fromisoformat(event["received_at"]),
        "properties": json.dumps(event["properties"], sort_keys=True),
    }


class Flusher:
    """Moves events from the Redis stream to Parquet files.

    Events are buffered and written when the buffer holds `max_events`, or when its oldest
    event has waited `max_seconds`: big files under load, bounded delay when quiet. Each flush
    writes one file per (project, date, hour) partition of the events' own timestamps, then
    acknowledges the stream entries. If the process dies between the write and the ack, the
    entries are delivered again and written again: at-least-once. Queries count distinct
    event ids, and compaction removes the duplicates.
    """

    def __init__(
        self,
        redis: Redis,
        storage: StorageProvider,
        *,
        stream: str,
        consumer: str,
        max_events: int,
        max_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._redis = redis
        self._storage = storage
        self._stream = stream
        self._consumer = consumer
        self._max_events = max_events
        self._max_seconds = max_seconds
        self._clock = clock
        self._buffer: list[tuple[str, dict[str, Any]]] = []
        self._oldest: float | None = None
        # First read this consumer's own unacknowledged entries (from before a restart).
        self._read_from = "0"
        self._group_ready = False

    @property
    def buffered(self) -> int:
        return len(self._buffer)

    async def _ensure_group(self) -> None:
        if self._group_ready:
            return
        try:
            await self._redis.xgroup_create(self._stream, GROUP, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._group_ready = True

    async def poll(self, block_ms: int | None = None) -> int:
        """Reads up to the free buffer space. Returns how many events were read."""
        await self._ensure_group()
        space = self._max_events - len(self._buffer)
        if space <= 0:
            return 0
        try:
            response = await self._redis.xreadgroup(
                GROUP, self._consumer, {self._stream: self._read_from}, count=space, block=block_ms
            )
        except ResponseError as exc:
            if "NOGROUP" in str(exc):  # stream deleted (e.g. Redis flushed): recreate next time
                self._group_ready = False
                return 0
            raise
        entries = response[0][1] if response else []
        if self._read_from == "0" and not entries:
            self._read_from = ">"  # own backlog done; from now on, new entries only
        for entry_id, fields in entries:
            if self._oldest is None:
                self._oldest = self._clock()
            self._buffer.append((entry_id, json.loads(fields["event"])))
        return len(entries)

    def should_flush(self) -> bool:
        if not self._buffer:
            return False
        waited = self._clock() - (self._oldest or self._clock())
        return len(self._buffer) >= self._max_events or waited >= self._max_seconds

    async def flush(self) -> list[str]:
        """Writes the buffer, one file per partition, then acks. Returns the keys written."""
        if not self._buffer:
            return []
        partitions: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for _, event in self._buffer:
            timestamp = datetime.fromisoformat(event["timestamp"])
            partitions[partition_prefix(event["project_id"], timestamp)].append(to_row(event))

        written = []
        for prefix, rows in partitions.items():
            key = f"{prefix}/part-{uuid.uuid4().hex}.parquet"
            await self._storage.put(key, await asyncio.to_thread(to_parquet, rows))
            written.append(key)

        ids = [entry_id for entry_id, _ in self._buffer]
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.xack(self._stream, GROUP, *ids)
            pipe.xdel(self._stream, *ids)  # acked entries are not needed in the stream any more
            await pipe.execute()
        logger.info("Flushed %d events into %d file(s)", len(ids), len(written))
        self._buffer, self._oldest = [], None
        return written

    async def run_once(self, block_ms: int | None = None) -> list[str]:
        await self.poll(block_ms)
        return await self.flush() if self.should_flush() else []
