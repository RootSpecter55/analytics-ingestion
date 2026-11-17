import os
from datetime import UTC, datetime, timedelta

from httpx import AsyncClient

from app.core.redis import get_redis
from tests.integration.conftest import ADMIN, create_project, event, register_schema, send

STREAM = os.environ["EVENTS_STREAM"]
SIGNUP_SCHEMA = {
    "type": "object",
    "properties": {"plan": {"enum": ["free", "pro"]}, "seats": {"type": "integer", "minimum": 1}},
    "required": ["plan"],
}


class TestBatch:
    async def test_reports_every_event_by_index_and_accepts_only_valid_new_ones(
        self, client: AsyncClient
    ) -> None:
        project = await create_project(client)
        await register_schema(client, project, "signup", SIGNUP_SCHEMA)
        first = event("signup", properties={"plan": "pro", "seats": 3}, event_id="evt-1")
        malformed = event("signup", properties={"plan": "free"})
        del malformed["userId"]

        result = await send(
            client,
            project,
            [
                first,  # 0 valid
                event("signup", properties={"plan": "gold", "seats": 0}),  # 1 breaks the schema
                malformed,  # 2 envelope invalid
                event("purchase"),  # 3 no schema registered
                {**first},  # 4 same id again in the same batch
                event(
                    "signup", at=datetime.now(UTC) + timedelta(hours=1), properties={"plan": "free"}
                ),
            ],
        )

        assert (result["accepted"], result["duplicates"], result["invalid"]) == (1, 1, 4)
        by_index = {r["index"]: r for r in result["results"]}
        assert by_index[0] == {"index": 0, "status": "accepted", "eventId": "evt-1", "errors": None}
        assert by_index[1]["errors"] == [
            "properties.plan: 'gold' is not one of ['free', 'pro']",
            "properties.seats: 0 is less than the minimum of 1",
        ]
        assert by_index[2]["errors"] == ["userId: Field required"]
        assert by_index[3]["errors"] == ["name: no schema is registered for event 'purchase'"]
        assert by_index[4]["status"] == "duplicate"
        assert by_index[5]["errors"] == ["timestamp: is in the future"]
        assert await get_redis().xlen(STREAM) == 1

    async def test_drops_an_event_id_seen_in_an_earlier_batch(self, client: AsyncClient) -> None:
        project = await create_project(client, "page_view")
        same = event("page_view", event_id="repeat-me")
        assert (await send(client, project, [same]))["accepted"] == 1
        again = await send(client, project, [same])
        assert again["results"][0]["status"] == "duplicate"
        ttl = await get_redis().ttl(f"dedup:{project.id}:repeat-me")
        assert 86_000 < ttl <= 86_400

    async def test_an_invalid_event_does_not_use_up_its_id(self, client: AsyncClient) -> None:
        project = await create_project(client)
        await register_schema(client, project, "signup", SIGNUP_SCHEMA)
        bad = event("signup", properties={}, event_id="fix-and-resend")
        assert (await send(client, project, [bad]))["invalid"] == 1
        fixed = {**bad, "properties": {"plan": "free"}}
        assert (await send(client, project, [fixed]))["accepted"] == 1

    async def test_the_same_id_in_two_projects_is_two_events(self, client: AsyncClient) -> None:
        a, b = await create_project(client, "x"), await create_project(client, "x")
        assert (await send(client, a, [event("x", event_id="shared")]))["accepted"] == 1
        assert (await send(client, b, [event("x", event_id="shared")]))["accepted"] == 1

    async def test_validates_against_an_explicit_schema_version(self, client: AsyncClient) -> None:
        project = await create_project(client)
        await register_schema(client, project, "signup", {"type": "object", "required": ["plan"]})
        await register_schema(
            client, project, "signup", SIGNUP_SCHEMA | {"required": ["plan", "seats"]}
        )
        old_shape = event("signup", properties={"plan": "free"})

        latest = await send(client, project, [old_shape])
        assert latest["results"][0]["errors"] == ["properties: 'seats' is a required property"]
        pinned = await send(client, project, [{**old_shape, "schemaVersion": 1}])
        assert pinned["accepted"] == 1
        missing = await send(client, project, [{**event("signup"), "schemaVersion": 9}])
        assert missing["results"][0]["errors"] == ["schemaVersion: 'signup' has no version 9"]

    async def test_limits_the_batch_size(self, client: AsyncClient) -> None:
        project = await create_project(client, "x")
        too_many = await client.post(
            "/v1/batch", headers=project.write, json={"events": [event("x")] * 501}
        )
        assert too_many.status_code == 413
        empty = await client.post("/v1/batch", headers=project.write, json={"events": []})
        assert empty.status_code == 400


class TestKeys:
    async def test_write_and_read_keys_cannot_be_swapped(self, client: AsyncClient) -> None:
        project = await create_project(client, "x")
        response = await client.post(
            "/v1/batch", headers=project.read, json={"events": [event("x")]}
        )
        assert response.status_code == 401
        query = await client.get(
            "/v1/queries/unique-users?from=2026-01-01&to=2026-01-02", headers=project.write
        )
        assert query.status_code == 401
        assert (
            await client.post("/v1/projects", headers=project.write, json={"name": "x"})
        ).status_code == 401


class TestSchemas:
    async def test_versions_increase_and_can_be_listed(self, client: AsyncClient) -> None:
        project = await create_project(client)
        v1 = await register_schema(client, project, "signup", {"type": "object"})
        v2 = await register_schema(client, project, "signup", SIGNUP_SCHEMA)
        assert (v1["version"], v2["version"]) == (1, 2)
        assert v2["schema"] == SIGNUP_SCHEMA

        listing = await client.get(
            f"/v1/projects/{project.id}/event-schemas?eventName=signup&pageSize=1", headers=ADMIN
        )
        assert listing.json()["meta"]["total"] == 2
        one = await client.get(
            f"/v1/projects/{project.id}/event-schemas/signup/versions/2", headers=ADMIN
        )
        assert one.json()["data"]["version"] == 2

    async def test_rejects_invalid_schemas_and_unknown_projects(self, client: AsyncClient) -> None:
        project = await create_project(client)
        url = f"/v1/projects/{project.id}/event-schemas"
        broken = await client.post(
            url, headers=ADMIN, json={"eventName": "a", "schema": {"type": "nonsense"}}
        )
        assert broken.json()["error"]["code"] == "INVALID_SCHEMA"
        not_object = await client.post(
            url, headers=ADMIN, json={"eventName": "a", "schema": {"type": "string"}}
        )
        assert not_object.status_code == 400
        bad_name = await client.post(
            url, headers=ADMIN, json={"eventName": "Bad Name!", "schema": {}}
        )
        assert bad_name.status_code == 400
        unknown = await client.post(
            "/v1/projects/00000000-0000-4000-8000-000000000000/event-schemas",
            headers=ADMIN,
            json={"eventName": "a", "schema": {"type": "object"}},
        )
        assert unknown.status_code == 404
