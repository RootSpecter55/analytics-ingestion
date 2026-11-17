import asyncio
import os
import shutil
import subprocess
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from app.container import Container, build_container
from app.core.config import get_settings
from app.core.db import get_engine
from app.core.redis import get_redis
from app.main import create_app
from app.modules.ingestion.flusher import Flusher

ADMIN = {"Authorization": f"Bearer {os.environ['ADMIN_API_KEY']}"}
OPEN_SCHEMA = {"type": "object"}


async def _create_database_if_missing() -> None:
    url = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
    base, db_name = url.rsplit("/", 1)
    connection = await asyncpg.connect(f"{base}/postgres")
    try:
        exists = await connection.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", db_name)
        if not exists:
            await connection.execute(f'CREATE DATABASE "{db_name}"')
    finally:
        await connection.close()


@pytest.fixture(scope="session", autouse=True)
async def database() -> AsyncIterator[None]:
    """Creates the test database if needed and applies migrations once per session."""
    await _create_database_if_missing()
    await asyncio.to_thread(
        subprocess.run,
        ["alembic", "upgrade", "head"],
        check=True,
        capture_output=True,
        env=os.environ,
    )
    yield
    await get_engine().dispose()
    await get_redis().aclose()


@pytest.fixture(autouse=True)
async def clean_state() -> None:
    """Every test starts with no projects, an empty Redis db and an empty data directory."""
    async with get_engine().begin() as connection:
        await connection.execute(text("TRUNCATE projects CASCADE"))
    await get_redis().flushdb()
    await asyncio.to_thread(_empty_directory, Path(os.environ["LOCAL_DATA_DIR"]))


def _empty_directory(root: Path) -> None:
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)


@pytest.fixture(scope="session")
def container() -> Container:
    return build_container(get_settings())


@pytest.fixture(scope="session")
def app(container: Container) -> Any:
    return create_app(container)


@pytest.fixture
async def client(app: Any) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def make_flusher(container: Container, *, max_events: int = 1000, clock: Any = None) -> Flusher:
    return Flusher(
        get_redis(),
        container.storage,
        stream=os.environ["EVENTS_STREAM"],
        consumer="flusher-test",
        max_events=max_events,
        max_seconds=60,
        **({"clock": clock} if clock else {}),
    )


async def flush_everything(flusher: Flusher) -> list[str]:
    """Reads until the stream is empty, then writes whatever is buffered."""
    while await flusher.poll():
        pass
    return await flusher.flush()


class Project:
    def __init__(self, project_id: str, write_key: str, read_key: str) -> None:
        self.id = project_id
        self.write = {"Authorization": f"Bearer {write_key}"}
        self.read = {"Authorization": f"Bearer {read_key}"}


async def create_project(client: AsyncClient, *event_names: str) -> Project:
    """A project, with an open schema registered for each event name given."""
    response = await client.post("/v1/projects", headers=ADMIN, json={"name": "Shop"})
    assert response.status_code == 201, response.text
    data = response.json()["data"]
    project = Project(data["project"]["id"], data["writeKey"], data["readKey"])
    for name in event_names:
        await register_schema(client, project, name, OPEN_SCHEMA)
    return project


async def register_schema(
    client: AsyncClient, project: Project, name: str, schema: dict[str, Any]
) -> dict[str, Any]:
    response = await client.post(
        f"/v1/projects/{project.id}/event-schemas",
        headers=ADMIN,
        json={"eventName": name, "schema": schema},
    )
    assert response.status_code == 201, response.text
    return response.json()["data"]


def event(
    name: str,
    user: str = "u1",
    at: datetime | None = None,
    properties: dict[str, Any] | None = None,
    event_id: str | None = None,
) -> dict[str, Any]:
    return {
        "eventId": event_id or uuid.uuid4().hex,
        "name": name,
        "userId": user,
        "timestamp": (at or datetime.now(UTC) - timedelta(minutes=1)).isoformat(),
        "properties": properties or {},
    }


async def send(
    client: AsyncClient, project: Project, events: list[dict[str, Any]]
) -> dict[str, Any]:
    response = await client.post("/v1/batch", headers=project.write, json={"events": events})
    assert response.status_code == 200, response.text
    return response.json()["data"]
