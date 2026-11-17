from app.core.config import Settings
from app.providers.query_engine.athena_engine import AthenaQueryEngine
from app.providers.query_engine.base import QueryEngine
from app.providers.query_engine.duckdb_engine import DuckDbQueryEngine


# Factory: picks the query engine from settings; the queries module sees only the interface.
def create_query_engine(settings: Settings) -> QueryEngine:
    if settings.query_engine == "athena":
        # Presence of these values is checked by Settings validation.
        return AthenaQueryEngine(
            database=settings.athena_database or "",
            table=settings.athena_table,
            workgroup=settings.athena_workgroup,
            output_location=settings.athena_output_location or "",
            region=settings.aws_region or "",
        )
    return DuckDbQueryEngine(settings.local_data_dir)
