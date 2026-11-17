from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.base_repository import BaseRepository
from app.modules.projects.models import Project


class ProjectRepository(BaseRepository[Project]):
    model = Project

    async def by_write_key_hash(self, session: AsyncSession, key_hash: str) -> Project | None:
        return await session.scalar(select(Project).where(Project.write_key_hash == key_hash))

    async def by_read_key_hash(self, session: AsyncSession, key_hash: str) -> Project | None:
        return await session.scalar(select(Project).where(Project.read_key_hash == key_hash))
