from typing import Any

from app.core.pagination import PageParams, data_envelope, page_envelope
from app.modules.projects.schemas import CreateProjectRequest
from app.modules.projects.service import ProjectsService


class ProjectsController:
    def __init__(self, service: ProjectsService) -> None:
        self._service = service

    async def create(self, body: CreateProjectRequest) -> dict[str, Any]:
        return data_envelope((await self._service.create(body)).to_json())

    async def list(self, page: PageParams) -> dict[str, Any]:
        items, total = await self._service.list(page)
        return page_envelope([p.to_json() for p in items], total, page)
