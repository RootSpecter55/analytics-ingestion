"""Measures ingestion throughput and how long events take to become queryable.

    docker compose up --build
    uv run python -m scripts.throughput                       # 50,000 events
    EVENTS=200000 BATCH_SIZE=500 CONCURRENCY=8 uv run python -m scripts.throughput

1. Creates a fresh demo project.
2. Sends EVENTS valid, unique events (timestamps in the last minute) in batches, CONCURRENCY
   requests at a time, and records each request's latency.
3. Polls the event-counts query until every event is visible: how long after the last batch the
   flusher had written them all. The flusher's FLUSH_MAX_SECONDS bounds this when traffic stops.

Prints events per second accepted, request latency percentiles and time to queryable. Everything
runs on one machine: the numbers describe this setup only.
"""

import asyncio
import os
import random
import statistics
import time
from datetime import UTC, datetime

import httpx

from scripts import load_generator
from scripts.load_generator import create_demo_project, make_event

API_URL = os.environ.get("API_URL", "http://localhost:8000")
EVENTS = int(os.environ.get("EVENTS", "50000"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "4"))
QUERY_TIMEOUT = float(os.environ.get("QUERY_TIMEOUT", "300"))


async def main() -> None:
    # Clean traffic only: no late, duplicate or invalid events, so every event should arrive.
    load_generator.LATE_SHARE = load_generator.DUPLICATE_SHARE = load_generator.INVALID_SHARE = 0
    async with httpx.AsyncClient(base_url=API_URL, timeout=60) as client:
        write_key, read_key = await create_demo_project(client)
        write = {"Authorization": f"Bearer {write_key}"}
        latencies: list[float] = []
        accepted = 0
        remaining = EVENTS

        async def sender(seed: int) -> None:
            nonlocal accepted, remaining
            rng, recent = random.Random(seed), []
            while remaining > 0:
                size = min(BATCH_SIZE, remaining)
                remaining -= size
                batch = [make_event(rng, recent) for _ in range(size)]
                started = time.perf_counter()
                response = await client.post("/v1/batch", headers=write, json={"events": batch})
                latencies.append(time.perf_counter() - started)
                accepted += response.json()["data"]["accepted"]

        started = time.perf_counter()
        await asyncio.gather(*(sender(seed) for seed in range(CONCURRENCY)))
        sent_after = time.perf_counter() - started

        today = datetime.now(UTC).date().isoformat()
        read = {"Authorization": f"Bearer {read_key}"}
        visible = 0
        while time.perf_counter() - started < sent_after + QUERY_TIMEOUT:
            response = await client.get(
                f"/v1/queries/event-counts?from={today}&to={today}", headers=read
            )
            visible = sum(row["events"] for row in response.json()["data"])
            if visible >= accepted:
                break
            await asyncio.sleep(0.5)
        queryable_after = time.perf_counter() - started - sent_after

    ordered = sorted(latencies)
    print(f"events: {EVENTS}, batch: {BATCH_SIZE}, concurrency: {CONCURRENCY}")
    print(f"accepted: {accepted} in {sent_after:.1f} s = {accepted / sent_after:.0f} events/s")
    print(
        f"batch latency: median {statistics.median(ordered) * 1000:.0f} ms, "
        f"p95 {ordered[int(0.95 * (len(ordered) - 1))] * 1000:.0f} ms"
    )
    print(f"queryable: {visible}/{accepted} events, {queryable_after:.1f} s after the last batch")


if __name__ == "__main__":
    asyncio.run(main())
