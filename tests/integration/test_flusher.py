import asyncio
import contextlib
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from httpx import AsyncClient

from app.container import Container
from app.core.lake import read_parquet
from app.core.redis import get_redis
from app.modules.ingestion.compaction import Compactor
from app.modules.ingestion.flusher import GROUP, Flusher
from tests.integration.conftest import (
    FakeClock,
    create_project,
    event,
    flush_everything,
    make_flusher,
    send,
)

STREAM = os.environ["EVENTS_STREAM"]


class CrashBeforeAck(Flusher):
    """Writes the files, then dies before acknowledging the stream entries."""

    async def _ack(self, ids: list[str]) -> None:
        raise ConnectionError("process killed")


async def _rows(container: Container, keys: list[str]) -> int:
    total = 0
    for key in keys:
        total += read_parquet(await container.storage.get(key)).num_rows
    return total


async def _pending() -> int:
    return (await get_redis().xpending(STREAM, GROUP))["pending"]


class TestFlushTriggers:
    async def test_flushes_when_the_buffer_is_full(
        self, client: AsyncClient, container: Container
    ) -> None:
        project = await create_project(client, "x")
        await send(client, project, [event("x") for _ in range(3)])
        flusher = make_flusher(container, max_events=3)

        written = await flusher.run_once()

        assert len(written) == 1
        assert await _rows(container, written) == 3
        assert await _pending() == 0
        assert await get_redis().xlen(STREAM) == 0  # acked entries are removed

    async def test_flushes_when_the_oldest_event_has_waited_long_enough(
        self, client: AsyncClient, container: Container
    ) -> None:
        project = await create_project(client, "x")
        await send(client, project, [event("x"), event("x")])
        clock = FakeClock()
        flusher = make_flusher(container, max_events=100, clock=clock)

        assert await flusher.run_once() == []  # 2 of 100, no time passed
        clock.now += 59
        assert await flusher.run_once() == []
        clock.now += 1
        assert len(await flusher.run_once()) == 1
        assert flusher.buffered == 0


class TestPartitioning:
    async def test_each_event_lands_in_the_hour_of_its_own_timestamp(
        self, client: AsyncClient, container: Container
    ) -> None:
        project = await create_project(client, "x")
        now = datetime.now(UTC).replace(minute=30, second=0, microsecond=0) - timedelta(hours=2)
        late = now - timedelta(days=3)  # arrives now, happened three days ago
        await send(client, project, [event("x", at=now), event("x", at=now), event("x", at=late)])

        written = await flush_everything(make_flusher(container))

        partitions = sorted(key.rsplit("/", 1)[0] for key in written)
        prefix = f"events/project_id={project.id}"
        assert partitions == sorted(
            [
                f"{prefix}/date={now:%Y-%m-%d}/hour={now:%H}",
                f"{prefix}/date={late:%Y-%m-%d}/hour={late:%H}",
            ]
        )
        late_rows = read_parquet(
            await container.storage.get(
                next(k for k in written if f"date={late:%Y-%m-%d}/hour={late:%H}" in k)
            )
        )
        assert late_rows.column("timestamp").to_pylist() == [late]


class TestCrashRecovery:
    async def test_a_crash_before_the_ack_rewrites_the_events_but_queries_count_them_once(
        self, client: AsyncClient, container: Container
    ) -> None:
        project = await create_project(client, "x")
        at = datetime.now(UTC) - timedelta(hours=3)
        await send(client, project, [event("x", user=f"u{i}", at=at) for i in range(5)])

        crashing = CrashBeforeAck(
            get_redis(),
            container.storage,
            stream=STREAM,
            consumer="flusher-test",
            max_events=1000,
            max_seconds=60,
        )
        await crashing.poll()
        with contextlib.suppress(ConnectionError):
            await crashing.flush()
        assert await _pending() == 5  # written to Parquet, but still owed to the stream

        # The restarted flusher (same consumer name) reads its unacknowledged entries again.
        await flush_everything(make_flusher(container))
        assert await _pending() == 0
        files = await asyncio.to_thread(
            lambda: sorted(Path(os.environ["LOCAL_DATA_DIR"]).rglob("*.parquet"))
        )
        assert len(files) == 2
        assert (
            await _rows(
                container, [str(f.relative_to(os.environ["LOCAL_DATA_DIR"])) for f in files]
            )
            == 10
        )

        day = at.date().isoformat()
        counts = await client.get(
            f"/v1/queries/event-counts?from={day}&to={day}", headers=project.read
        )
        assert counts.json()["data"] == [{"date": day, "events": 5}]

        # Compaction merges the two files and drops the duplicate rows.
        report = await Compactor(container.storage).compact()
        assert (report.files_in, report.files_out, report.rows_in, report.rows_out) == (2, 1, 10, 5)


class TestCompaction:
    async def test_merges_finished_hours_only(
        self, client: AsyncClient, container: Container
    ) -> None:
        project = await create_project(client, "x")
        past = datetime.now(UTC) - timedelta(hours=5)
        current = datetime.now(UTC) - timedelta(seconds=10)
        for _ in range(3):  # three small files in the past hour, three in the current one
            await send(client, project, [event("x", at=past), event("x", at=current)])
            await flush_everything(make_flusher(container))

        report = await Compactor(container.storage).compact()

        assert (report.partitions, report.files_in, report.files_out) == (1, 3, 1)
        assert (report.rows_in, report.rows_out) == (3, 3)
        keys = await container.storage.list(f"events/project_id={project.id}")
        current_hour = f"date={current:%Y-%m-%d}/hour={current:%H}"
        assert len([k for k in keys if current_hour in k]) == 3  # untouched
        assert len([k for k in keys if current_hour not in k]) == 1
