from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from httpx import AsyncClient

from app.container import Container
from tests.integration.conftest import (
    Project,
    create_project,
    event,
    flush_everything,
    make_flusher,
    send,
)

# Ten days ago, so the whole dataset (day 0 to day 7) is in the past.
D0 = (datetime.now(UTC) - timedelta(days=10)).date()


def at(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(D0.year, D0.month, D0.day, hour, minute, tzinfo=UTC) + timedelta(days=day)


DATASET = [
    # u1 goes through the funnel within half an hour.
    ("signup", "u1", at(0, 10)),
    ("view_pricing", "u1", at(0, 10, 5)),
    ("checkout", "u1", at(0, 10, 30)),
    # u2 checks out the next day: outside a 1 h window, inside a 2-day one.
    ("signup", "u2", at(0, 11)),
    ("view_pricing", "u2", at(0, 11, 10)),
    ("checkout", "u2", at(1, 12)),
    # u3 stops after viewing pricing.
    ("signup", "u3", at(0, 9)),
    ("view_pricing", "u3", at(0, 9, 30)),
    # u4 viewed pricing *before* signing up: does not count as step 2.
    ("view_pricing", "u4", at(0, 8)),
    ("signup", "u4", at(0, 8, 30)),
    # Coming back: u1 on day 1 and day 7, u2 on day 1, u3 on day 2.
    ("session_start", "u1", at(1, 9)),
    ("session_start", "u1", at(7, 9)),
    ("session_start", "u2", at(1, 15)),
    ("session_start", "u3", at(2, 15)),
]
NAMES = ("signup", "view_pricing", "checkout", "session_start")


@pytest.fixture
async def shop(client: AsyncClient, container: Container) -> Project:
    project = await create_project(client, *NAMES)
    result = await send(client, project, [event(n, user=u, at=t) for n, u, t in DATASET])
    assert result["accepted"] == len(DATASET)
    await flush_everything(make_flusher(container))
    return project


async def _post(client: AsyncClient, project: Project, path: str, body: dict[str, Any]) -> Any:
    response = await client.post(f"/v1/queries/{path}", headers=project.read, json=body)
    assert response.status_code == 200, response.text
    return response.json()["data"]


def _day(n: int) -> str:
    return (D0 + timedelta(days=n)).isoformat()


class TestCounts:
    async def test_daily_counts_include_empty_days_and_paginate(
        self, client: AsyncClient, shop: Project
    ) -> None:
        response = await client.get(
            f"/v1/queries/event-counts?event=signup&from={_day(0)}&to={_day(2)}", headers=shop.read
        )
        assert response.json()["data"] == [
            {"date": _day(0), "events": 4},
            {"date": _day(1), "events": 0},
            {"date": _day(2), "events": 0},
        ]
        all_events = await client.get(
            f"/v1/queries/event-counts?from={_day(0)}&to={_day(7)}&pageSize=3&page=1",
            headers=shop.read,
        )
        body = all_events.json()
        assert body["meta"] == {"page": 1, "pageSize": 3, "total": 8, "totalPages": 3}
        assert [row["events"] for row in body["data"]] == [9, 3, 1]

    async def test_unique_users(self, client: AsyncClient, shop: Project) -> None:
        everyone = await client.get(
            f"/v1/queries/unique-users?from={_day(0)}&to={_day(7)}", headers=shop.read
        )
        assert everyone.json()["data"]["users"] == 4
        buyers = await client.get(
            f"/v1/queries/unique-users?event=checkout&from={_day(0)}&to={_day(7)}",
            headers=shop.read,
        )
        assert buyers.json()["data"]["users"] == 2


class TestFunnel:
    async def test_counts_users_who_did_the_steps_in_order_within_the_window(
        self, client: AsyncClient, shop: Project
    ) -> None:
        body = {
            "steps": ["signup", "view_pricing", "checkout"],
            "from": _day(0),
            "to": _day(7),
            "windowSeconds": 3600,
        }
        one_hour = await _post(client, shop, "funnel", body)
        assert [(s["name"], s["users"], s["conversion"]) for s in one_hour] == [
            ("signup", 4, 1.0),
            ("view_pricing", 3, 0.75),  # u4 viewed pricing before signing up
            ("checkout", 1, 0.25),  # u2 took longer than an hour
        ]
        two_days = await _post(client, shop, "funnel", body | {"windowSeconds": 2 * 86400})
        assert [s["users"] for s in two_days] == [4, 3, 2]

    async def test_validates_steps_and_dates(self, client: AsyncClient, shop: Project) -> None:
        base = {"from": _day(0), "to": _day(1), "windowSeconds": 60}
        for steps in (["signup"], ["a", "b", "c", "d", "e", "f"], ["signup", "Bad Name"]):
            response = await client.post(
                "/v1/queries/funnel", headers=shop.read, json=base | {"steps": steps}
            )
            assert response.status_code == 400, steps
        backwards = await client.post(
            "/v1/queries/funnel",
            headers=shop.read,
            json=base | {"steps": ["a", "b"], "from": _day(2), "to": _day(1)},
        )
        assert backwards.status_code == 400


class TestRetention:
    async def test_day_n_retention_of_the_signup_cohort(
        self, client: AsyncClient, shop: Project
    ) -> None:
        points = await _post(
            client,
            shop,
            "retention",
            {
                "startEvent": "signup",
                "returnEvent": "session_start",
                "cohortFrom": _day(0),
                "cohortTo": _day(0),
                "days": [7, 1, 2, 1],
            },
        )
        assert points == [
            {"day": 1, "cohortSize": 4, "retained": 2, "rate": 0.5},
            {"day": 2, "cohortSize": 4, "retained": 1, "rate": 0.25},
            {"day": 7, "cohortSize": 4, "retained": 1, "rate": 0.25},
        ]


class TestSafety:
    async def test_values_are_bound_parameters_never_sql(
        self, container: Container, shop: Project
    ) -> None:
        """Calls the engine directly, skipping the API's validation, with text that would change
        the query if it were pasted into the SQL. It is compared as a plain value instead."""
        engine = container.queries._service._engine
        project_id = UUID(shop.id)
        attacks = [
            "signup' OR '1'='1",
            "x'; DROP TABLE events; --",
            'signup") OR 1=1 --',
        ]
        for attack in attacks:
            assert await engine.daily_counts(project_id, attack, D0, D0 + timedelta(days=7)) == []
            assert await engine.unique_users(project_id, attack, D0, D0 + timedelta(days=7)) == 0
            steps = await engine.funnel(
                project_id, ["signup", attack], D0, D0 + timedelta(days=7), 60
            )
            assert [s.users for s in steps] == [4, 0]
        # The real data is untouched and still answers normally.
        assert await engine.unique_users(project_id, None, D0, D0 + timedelta(days=7)) == 4

    async def test_the_api_refuses_anything_that_is_not_an_event_name(
        self, client: AsyncClient, shop: Project
    ) -> None:
        response = await client.get(
            "/v1/queries/unique-users",
            headers=shop.read,
            params={"event": "signup' OR 1=1 --", "from": _day(0), "to": _day(1)},
        )
        assert response.status_code == 400

    async def test_a_project_only_sees_its_own_events(
        self, client: AsyncClient, shop: Project
    ) -> None:
        other = await create_project(client, *NAMES)
        response = await client.get(
            f"/v1/queries/unique-users?from={_day(0)}&to={_day(7)}", headers=other.read
        )
        assert response.json()["data"]["users"] == 0

    async def test_ranges_are_limited(self, client: AsyncClient, shop: Project) -> None:
        too_long = date(2025, 1, 1), date(2026, 6, 1)
        response = await client.get(
            f"/v1/queries/unique-users?from={too_long[0]}&to={too_long[1]}", headers=shop.read
        )
        assert response.status_code == 400
