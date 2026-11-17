from app.core.auth import KeyKind, ProjectContext
from app.core.db import Database
from app.core.pagination import PageParams
from app.core.security import new_key, sha256_hex
from app.modules.projects.models import Project
from app.modules.projects.repository import ProjectRepository
from app.modules.projects.schemas import CreatedProject, CreateProjectRequest, ProjectOut


class ProjectsService:
    def __init__(self, db: Database, projects: ProjectRepository) -> None:
        self._db = db
        self._projects = projects

    async def create(self, request: CreateProjectRequest) -> CreatedProject:
        """Two keys: the write key goes into apps (it can only send events), the read key stays
        on the server side (it can only run queries). Only their hashes are stored."""
        write_key, read_key = new_key("wk"), new_key("rk")
        async with self._db.transaction() as session:
            project = await self._projects.add(
                session,
                Project(
                    name=request.name,
                    write_key_hash=sha256_hex(write_key),
                    read_key_hash=sha256_hex(read_key),
                ),
            )
        return CreatedProject(
            project=ProjectOut.model_validate(project), write_key=write_key, read_key=read_key
        )

    async def list(self, page: PageParams) -> tuple[list[ProjectOut], int]:
        async with self._db.transaction() as session:
            rows, total = await self._projects.list(
                session, order_by=Project.created_at, offset=page.offset, limit=page.page_size
            )
        return [ProjectOut.model_validate(p) for p in rows], total

    async def resolve(self, key: str, kind: KeyKind) -> ProjectContext | None:
        """Used by the auth dependencies. A write key is only looked up as a write key."""
        async with self._db.transaction() as session:
            lookup = (
                self._projects.by_write_key_hash
                if kind == "write"
                else self._projects.by_read_key_hash
            )
            project = await lookup(session, sha256_hex(key))
        return ProjectContext(project_id=project.id, name=project.name) if project else None
