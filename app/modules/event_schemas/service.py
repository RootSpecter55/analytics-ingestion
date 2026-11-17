from typing import Any
from uuid import UUID

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from sqlalchemy.exc import IntegrityError

from app.core.db import Database
from app.core.errors import AppError, ConflictError, NotFoundError
from app.core.pagination import PageParams
from app.modules.event_schemas.models import EventSchema
from app.modules.event_schemas.repository import EventSchemaRepository
from app.modules.event_schemas.schemas import EventSchemaOut
from app.modules.projects.repository import ProjectRepository


def check_schema(schema: dict[str, Any]) -> None:
    """A schema must itself be valid Draft 2020-12, and describe an object (properties)."""
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        raise AppError(400, "INVALID_SCHEMA", f"Not a valid JSON Schema: {exc.message}") from exc
    if schema.get("type") != "object":
        raise AppError(400, "INVALID_SCHEMA", 'The schema must have "type": "object"')


class EventSchemasService:
    def __init__(
        self, db: Database, schemas: EventSchemaRepository, projects: ProjectRepository
    ) -> None:
        self._db = db
        self._schemas = schemas
        self._projects = projects

    async def register(
        self, project_id: UUID, event_name: str, schema: dict[str, Any]
    ) -> EventSchemaOut:
        """Adds the next version. Existing versions never change, so events already validated
        against one stay explainable."""
        check_schema(schema)
        try:
            async with self._db.transaction() as session:
                if await self._projects.get(session, project_id) is None:
                    raise NotFoundError("Project")
                version = await self._schemas.latest_version(session, project_id, event_name) + 1
                row = await self._schemas.add(
                    session,
                    EventSchema(
                        project_id=project_id,
                        event_name=event_name,
                        version=version,
                        schema=schema,
                    ),
                )
        except IntegrityError as exc:
            # Two registrations raced for the same version number; the unique key let one win.
            raise ConflictError("Another version was registered at the same time; retry") from exc
        return EventSchemaOut.model_validate(row)

    async def list(
        self, project_id: UUID, event_name: str | None, page: PageParams
    ) -> tuple[list[EventSchemaOut], int]:
        filters = [EventSchema.project_id == project_id]
        if event_name:
            filters.append(EventSchema.event_name == event_name)
        async with self._db.transaction() as session:
            rows, total = await self._schemas.list(
                session,
                *filters,
                order_by=(EventSchema.event_name, EventSchema.version),
                offset=page.offset,
                limit=page.page_size,
            )
        return [EventSchemaOut.model_validate(r) for r in rows], total

    async def get(self, project_id: UUID, event_name: str, version: int) -> EventSchemaOut:
        async with self._db.transaction() as session:
            row = await self._schemas.get_version(session, project_id, event_name, version)
        if row is None:
            raise NotFoundError(f"Schema {event_name} v{version}")
        return EventSchemaOut.model_validate(row)
