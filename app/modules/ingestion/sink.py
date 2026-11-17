from redis.asyncio import Redis

# Atomically: remember the event id for TTL seconds, and only if it was new, add the event to
# the stream. Doing both in one script means a crash can never leave an id marked as seen
# without its event having been queued (which would silently drop the event on retry).
DEDUP_AND_APPEND = """
if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1]) then
  redis.call('XADD', KEYS[2], '*', 'event', ARGV[2])
  return 1
end
return 0
"""


class EventSink:
    """Accepts events into the Redis stream, dropping any event id seen in the last TTL."""

    def __init__(self, redis: Redis, stream: str, dedup_ttl_seconds: int) -> None:
        self._redis = redis
        self._stream = stream
        self._ttl = dedup_ttl_seconds
        self._script = redis.register_script(DEDUP_AND_APPEND)

    async def append(self, project_id: str, events: list[tuple[str, str]]) -> list[bool]:
        """`events` are (event id, JSON). Returns True for each event added, False for each
        duplicate. One round trip for the whole batch; the script runs once per event, in
        order, so a duplicate inside the same batch is caught too."""
        if not events:
            return []
        async with self._redis.pipeline(transaction=False) as pipe:
            for event_id, payload in events:
                await self._script(
                    keys=[f"dedup:{project_id}:{event_id}", self._stream],
                    args=[self._ttl, payload],
                    client=pipe,
                )
            results = await pipe.execute()
        return [result == 1 for result in results]
