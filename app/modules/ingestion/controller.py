from typing import Any

from app.core.auth import ProjectContext
from app.core.pagination import data_envelope
from app.modules.ingestion.schemas import BatchRequest
from app.modules.ingestion.service import IngestionService


class IngestionController:
    def __init__(self, service: IngestionService) -> None:
        self._service = service

    async def batch(self, project: ProjectContext, body: BatchRequest) -> dict[str, Any]:
        return data_envelope((await self._service.ingest(project, body.events)).to_json())
