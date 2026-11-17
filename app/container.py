from dataclasses import dataclass
from typing import Any

from app.core.auth import make_admin_auth, make_project_auth
from app.core.config import Settings
from app.core.db import Database, get_engine
from app.core.redis import get_redis
from app.modules.event_schemas.controller import EventSchemasController
from app.modules.event_schemas.repository import EventSchemaRepository
from app.modules.event_schemas.service import EventSchemasService
from app.modules.ingestion.controller import IngestionController
from app.modules.ingestion.service import IngestionService
from app.modules.ingestion.sink import EventSink
from app.modules.projects.controller import ProjectsController
from app.modules.projects.repository import ProjectRepository
from app.modules.projects.service import ProjectsService
from app.modules.queries.controller import QueriesController
from app.modules.queries.service import QueriesService
from app.providers.query_engine.base import QueryEngine
from app.providers.query_engine.factory import create_query_engine
from app.providers.storage.base import StorageProvider
from app.providers.storage.factory import create_storage


@dataclass
class Container:
    settings: Settings
    require_admin: Any
    write_key: Any
    read_key: Any
    projects: ProjectsController
    event_schemas: EventSchemasController
    ingestion: IngestionController
    queries: QueriesController
    # Used by the flusher and the CLIs.
    storage: StorageProvider


def build_container(settings: Settings, *, query_engine: QueryEngine | None = None) -> Container:
    """Composition root: wires every repository, service and controller in one place."""
    db = Database(get_engine())
    project_repo, schema_repo = ProjectRepository(), EventSchemaRepository()
    projects = ProjectsService(db, project_repo)
    ingestion = IngestionService(
        db,
        schema_repo,
        EventSink(get_redis(), settings.events_stream, settings.dedup_ttl_seconds),
        max_batch_size=settings.max_batch_size,
        max_future_seconds=settings.max_future_seconds,
        max_event_age_days=settings.max_event_age_days,
    )
    return Container(
        settings=settings,
        require_admin=make_admin_auth(settings.admin_api_key),
        write_key=make_project_auth(projects.resolve, "write"),
        read_key=make_project_auth(projects.resolve, "read"),
        projects=ProjectsController(projects),
        event_schemas=EventSchemasController(EventSchemasService(db, schema_repo, project_repo)),
        ingestion=IngestionController(ingestion),
        queries=QueriesController(QueriesService(query_engine or create_query_engine(settings))),
        storage=create_storage(settings),
    )
