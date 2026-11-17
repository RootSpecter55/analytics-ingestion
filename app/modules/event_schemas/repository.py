from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.base_repository import BaseRepository
from app.modules.event_schemas.models import EventSchema


class EventSchemaRepository(BaseRepository[EventSchema]):
    model = EventSchema

    async def latest_version(self, session: AsyncSession, project_id: UUID, name: str) -> int:
        version = await session.scalar(
            select(func.max(EventSchema.version)).where(
                EventSchema.project_id == project_id, EventSchema.event_name == name
            )
        )
        return version or 0

    async def get_version(
        self, session: AsyncSession, project_id: UUID, name: str, version: int
    ) -> EventSchema | None:
        return await session.scalar(
            select(EventSchema).where(
                EventSchema.project_id == project_id,
                EventSchema.event_name == name,
                EventSchema.version == version,
            )
        )

    async def for_names(
        self, session: AsyncSession, project_id: UUID, names: set[str]
    ) -> list[EventSchema]:
        """Every version of the given event names, so one query serves a whole batch."""
        rows = await session.scalars(
            select(EventSchema).where(
                EventSchema.project_id == project_id, EventSchema.event_name.in_(names)
            )
        )
        return list(rows)
