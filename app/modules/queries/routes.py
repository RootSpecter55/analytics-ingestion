from collections.abc import Callable
from datetime import date
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from app.core.auth import ProjectContext
from app.core.errors import AppError
from app.core.pagination import PageParams, page_params
from app.modules.event_schemas.schemas import EVENT_NAME_PATTERN
from app.modules.queries.controller import QueriesController
from app.modules.queries.schemas import FunnelRequest, RetentionRequest, check_range


def queries_router(controller: QueriesController, read_key: Callable[..., Any]) -> APIRouter:
    router = APIRouter(prefix="/v1/queries", tags=["queries"])
    Project = Annotated[ProjectContext, Depends(read_key)]
    Event = Annotated[str | None, Query(pattern=EVENT_NAME_PATTERN)]
    From = Annotated[date, Query(alias="from")]

    def date_range(start: date, end: date) -> None:
        try:
            check_range(start, end)
        except ValueError as exc:
            raise AppError(400, "VALIDATION_ERROR", str(exc)) from exc

    @router.get("/event-counts", summary="Events per day (distinct event ids), paginated by day")
    async def event_counts(
        project: Project,
        start: From,
        to: date,
        page: Annotated[PageParams, Depends(page_params)],
        event: Event = None,
    ) -> dict[str, Any]:
        date_range(start, to)
        return await controller.event_counts(project, event, start, to, page)

    @router.get("/unique-users", summary="Distinct users in a date range")
    async def unique_users(
        project: Project, start: From, to: date, event: Event = None
    ) -> dict[str, Any]:
        date_range(start, to)
        return await controller.unique_users(project, event, start, to)

    @router.post("/funnel", summary="Users reaching each of 2-5 steps, in order, within a window")
    async def funnel(project: Project, body: FunnelRequest) -> dict[str, Any]:
        return await controller.funnel(project, body)

    @router.post("/retention", summary="Day-N retention of the users who first did an event")
    async def retention(project: Project, body: RetentionRequest) -> dict[str, Any]:
        return await controller.retention(project, body)

    return router
