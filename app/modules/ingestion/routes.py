from collections.abc import Callable
from typing import Annotated, Any

from fastapi import APIRouter, Depends

from app.core.auth import ProjectContext
from app.modules.ingestion.controller import IngestionController
from app.modules.ingestion.schemas import BatchRequest


def ingestion_router(controller: IngestionController, write_key: Callable[..., Any]) -> APIRouter:
    router = APIRouter(prefix="/v1", tags=["ingestion"])

    @router.post(
        "/batch",
        summary="Send up to MAX_BATCH_SIZE events; each is validated and reported on its own",
    )
    async def batch(
        project: Annotated[ProjectContext, Depends(write_key)], body: BatchRequest
    ) -> dict[str, Any]:
        return await controller.batch(project, body)

    return router
