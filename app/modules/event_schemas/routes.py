from collections.abc import Callable
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query

from app.core.pagination import PageParams, page_params
from app.modules.event_schemas.controller import EventSchemasController
from app.modules.event_schemas.schemas import EVENT_NAME_PATTERN, RegisterSchemaRequest


def event_schemas_router(
    controller: EventSchemasController, require_admin: Callable[..., Any]
) -> APIRouter:
    router = APIRouter(
        prefix="/v1/projects/{project_id}/event-schemas",
        tags=["event schemas"],
        dependencies=[Depends(require_admin)],
    )
    EventName = Annotated[str, Path(pattern=EVENT_NAME_PATTERN)]

    @router.post("", status_code=201, summary="Register the next schema version for an event")
    async def register(project_id: UUID, body: RegisterSchemaRequest) -> dict[str, Any]:
        return await controller.register(project_id, body)

    @router.get("", summary="List schema versions")
    async def list_schemas(
        project_id: UUID,
        page: Annotated[PageParams, Depends(page_params)],
        event_name: Annotated[
            str | None, Query(alias="eventName", pattern=EVENT_NAME_PATTERN)
        ] = None,
    ) -> dict[str, Any]:
        return await controller.list(project_id, event_name, page)

    @router.get("/{event_name}/versions/{version}", summary="One schema version")
    async def get(
        project_id: UUID, event_name: EventName, version: Annotated[int, Path(ge=1)]
    ) -> dict[str, Any]:
        return await controller.get(project_id, event_name, version)

    return router
