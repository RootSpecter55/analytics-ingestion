"""Sends synthetic product events to a running API, to see the pipeline work under load.

    uv run python -m scripts.load_generator                      # 20 s, as fast as possible
    DURATION=60 RATE=2000 USERS=5000 uv run python -m scripts.load_generator

Creates a demo project with schemas for page_view, signup and checkout (unless WRITE_KEY is
given), then posts batches of BATCH_SIZE events. A share of events is deliberately late
(LATE_SHARE, up to 2 days old), duplicated (DUPLICATE_SHARE) or invalid (INVALID_SHARE), so dedup,
validation and partitioning are exercised too. Prints what the API reported.
"""

import asyncio
import os
import random
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

API_URL = os.environ.get("API_URL", "http://localhost:8000")
ADMIN_API_KEY = os.environ.get("ADMIN_API_KEY", "change-me-admin-key-at-least-32-characters")
DURATION = float(os.environ.get("DURATION", "20"))
RATE = float(os.environ.get("RATE", "0"))  # events per second; 0 = no limit
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "4"))
USERS = int(os.environ.get("USERS", "1000"))
LATE_SHARE = float(os.environ.get("LATE_SHARE", "0.02"))
DUPLICATE_SHARE = float(os.environ.get("DUPLICATE_SHARE", "0.01"))
INVALID_SHARE = float(os.environ.get("INVALID_SHARE", "0.01"))

SCHEMAS: dict[str, dict[str, Any]] = {
    "page_view": {
        "type": "object",
        "properties": {"path": {"type": "string"}},
        "required": ["path"],
    },
    "signup": {
        "type": "object",
        "properties": {"plan": {"enum": ["free", "pro"]}},
        "required": ["plan"],
    },
    "checkout": {
        "type": "object",
        "properties": {"amountCents": {"type": "integer", "minimum": 1}},
        "required": ["amountCents"],
    },
}


def make_event(rng: random.Random, recent_ids: list[str]) -> dict[str, Any]:
    if recent_ids and rng.random() < DUPLICATE_SHARE:
        event_id = rng.choice(recent_ids)
    else:
        event_id = uuid.uuid4().hex
        recent_ids.append(event_id)
        del recent_ids[:-1000]
    name = rng.choices(["page_view", "signup", "checkout"], weights=[90, 7, 3])[0]
    age = timedelta(seconds=rng.uniform(0, 60))
    if rng.random() < LATE_SHARE:
        age = timedelta(hours=rng.uniform(1, 48))
    properties: dict[str, Any] = {
        "page_view": {"path": rng.choice(["/", "/pricing", "/docs", "/blog"])},
        "signup": {"plan": rng.choice(["free", "pro"])},
        "checkout": {"amountCents": rng.randint(500, 50_000)},
    }[name]
    if rng.random() < INVALID_SHARE:
        properties = {}  # breaks the "required" rule
    return {
        "eventId": event_id,
        "name": name,
        "userId": f"user-{rng.randrange(USERS)}",
        "timestamp": (datetime.now(UTC) - age).isoformat(),
        "properties": properties,
    }


async def create_demo_project(client: httpx.AsyncClient) -> tuple[str, str]:
    """Returns (write key, read key)."""
    admin = {"Authorization": f"Bearer {ADMIN_API_KEY}"}
    created = await client.post("/v1/projects", headers=admin, json={"name": "load test"})
    created.raise_for_status()
    data = created.json()["data"]
    for name, schema in SCHEMAS.items():
        response = await client.post(
            f"/v1/projects/{data['project']['id']}/event-schemas",
            headers=admin,
            json={"eventName": name, "schema": schema},
        )
        response.raise_for_status()
    return data["writeKey"], data["readKey"]


async def main() -> None:
    async with httpx.AsyncClient(base_url=API_URL, timeout=30) as client:
        write_key = os.environ.get("WRITE_KEY")
        if not write_key:
            write_key, read_key = await create_demo_project(client)
            print(f"Created a demo project. Read key (for queries): {read_key}")
        headers = {"Authorization": f"Bearer {write_key}"}
        totals = {"sent": 0, "accepted": 0, "duplicates": 0, "invalid": 0, "errors": 0}
        deadline = time.monotonic() + DURATION
        interval = BATCH_SIZE / RATE * CONCURRENCY if RATE else 0.0

        async def sender(seed: int) -> None:
            rng, recent = random.Random(seed), []
            while time.monotonic() < deadline:
                started = time.monotonic()
                batch = [make_event(rng, recent) for _ in range(BATCH_SIZE)]
                response = await client.post("/v1/batch", headers=headers, json={"events": batch})
                totals["sent"] += len(batch)
                if response.status_code != 200:
                    totals["errors"] += 1
                    continue
                result = response.json()["data"]
                for key in ("accepted", "duplicates", "invalid"):
                    totals[key] += result[key]
                if interval:
                    await asyncio.sleep(max(0.0, interval - (time.monotonic() - started)))

        await asyncio.gather(*(sender(seed) for seed in range(CONCURRENCY)))
    print(totals)


if __name__ == "__main__":
    asyncio.run(main())
