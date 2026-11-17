import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, ForeignKey, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db import Base


class EventSchema(Base):
    """One version of the JSON Schema for an event's properties. Versions are never edited:
    registering a change adds the next version."""

    __tablename__ = "event_schemas"
    __table_args__ = (UniqueConstraint("project_id", "event_name", "version"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    event_name: Mapped[str] = mapped_column(String(100))
    version: Mapped[int]
    schema: Mapped[dict[str, Any]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
