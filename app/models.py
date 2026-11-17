"""Imports every ORM model so Base.metadata is complete (Alembic and tests use this)."""

from app.core.db import Base
from app.modules.event_schemas.models import EventSchema
from app.modules.projects.models import Project

__all__ = ["Base", "EventSchema", "Project"]
