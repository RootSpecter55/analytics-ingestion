from typing import Any
from uuid import UUID

from app.core.pagination import PageParams, data_envelope, page_envelope
from app.modules.event_schemas.schemas import RegisterSchemaRequest
from app.modules.event_schemas.service import EventSchemasService


class EventSchemasController:
    def __init__(self, service: EventSchemasService) -> None:
        self._service = service

    async def register(self, project_id: UUID, body: RegisterSchemaRequest) -> dict[str, Any]:
        created = await self._service.register(project_id, body.event_name, body.schema_)
        return data_envelope(created.to_json())

    async def list(
        self, project_id: UUID, event_name: str | None, page: PageParams
    ) -> dict[str, Any]:
        items, total = await self._service.list(project_id, event_name, page)
        return page_envelope([s.to_json() for s in items], total, page)

    async def get(self, project_id: UUID, event_name: str, version: int) -> dict[str, Any]:
        return data_envelope((await self._service.get(project_id, event_name, version)).to_json())
