from datetime import date
from typing import Any

from app.core.auth import ProjectContext
from app.core.pagination import PageParams, data_envelope, page_envelope
from app.modules.queries.schemas import FunnelRequest, RetentionRequest
from app.modules.queries.service import QueriesService


class QueriesController:
    def __init__(self, service: QueriesService) -> None:
        self._service = service

    async def event_counts(
        self, project: ProjectContext, event: str | None, start: date, end: date, page: PageParams
    ) -> dict[str, Any]:
        rows, total = await self._service.event_counts(project, event, start, end, page)
        return page_envelope([r.to_json() for r in rows], total, page)

    async def unique_users(
        self, project: ProjectContext, event: str | None, start: date, end: date
    ) -> dict[str, Any]:
        return data_envelope(
            (await self._service.unique_users(project, event, start, end)).to_json()
        )

    async def funnel(self, project: ProjectContext, body: FunnelRequest) -> dict[str, Any]:
        return data_envelope([s.to_json() for s in await self._service.funnel(project, body)])

    async def retention(self, project: ProjectContext, body: RetentionRequest) -> dict[str, Any]:
        return data_envelope([p.to_json() for p in await self._service.retention(project, body)])
