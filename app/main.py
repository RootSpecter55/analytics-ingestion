from fastapi import FastAPI

from app.container import Container, build_container
from app.core.config import get_settings
from app.core.errors import register_error_handlers
from app.core.logging import configure_logging
from app.core.pagination import data_envelope
from app.modules.event_schemas.routes import event_schemas_router
from app.modules.ingestion.routes import ingestion_router
from app.modules.projects.routes import projects_router
from app.modules.queries.routes import queries_router


def create_app(container: Container | None = None) -> FastAPI:
    configure_logging()
    c = container or build_container(get_settings())

    app = FastAPI(
        title="Analytics ingestion",
        version="1.0.0",
        description="Schema-validated events into Parquet, with funnel and retention queries.",
    )
    register_error_handlers(app)

    @app.get("/health", tags=["health"])
    async def health() -> dict[str, object]:
        return data_envelope({"status": "ok"})

    app.include_router(projects_router(c.projects, c.require_admin))
    app.include_router(event_schemas_router(c.event_schemas, c.require_admin))
    app.include_router(ingestion_router(c.ingestion, c.write_key))
    app.include_router(queries_router(c.queries, c.read_key))
    return app
