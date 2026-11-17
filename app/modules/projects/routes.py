from collections.abc import Callable
from typing import Annotated, Any

from fastapi import APIRouter, Depends

from app.core.pagination import PageParams, page_params
from app.modules.projects.controller import ProjectsController
from app.modules.projects.schemas import CreateProjectRequest


def projects_router(controller: ProjectsController, require_admin: Callable[..., Any]) -> APIRouter:
    router = APIRouter(
        prefix="/v1/projects", tags=["projects"], dependencies=[Depends(require_admin)]
    )

    @router.post("", status_code=201, summary="Create a project; its keys are returned once")
    async def create(body: CreateProjectRequest) -> dict[str, Any]:
        return await controller.create(body)

    @router.get("", summary="List projects")
    async def list_projects(page: Annotated[PageParams, Depends(page_params)]) -> dict[str, Any]:
        return await controller.list(page)

    return router
