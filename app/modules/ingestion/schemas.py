from datetime import datetime
from typing import Any, Literal

from pydantic import AwareDatetime, Field

from app.core.schemas import ApiModel
from app.modules.event_schemas.schemas import EVENT_NAME_PATTERN

# A hard cap for the request model; MAX_BATCH_SIZE (lower) is enforced by the service.
HARD_BATCH_CAP = 5000


class EventIn(ApiModel):
    """One event's envelope. Validated per event, so one bad event never sinks the batch."""

    event_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    name: str = Field(pattern=EVENT_NAME_PATTERN)
    user_id: str = Field(min_length=1, max_length=200)
    timestamp: AwareDatetime  # must carry a time zone; stored as UTC
    properties: dict[str, Any] = Field(default_factory=dict)
    # Validate against this version instead of the latest.
    schema_version: int | None = Field(default=None, ge=1)


class BatchRequest(ApiModel):
    # Raw objects: each is validated on its own, so their errors can be reported by index.
    events: list[dict[str, Any]] = Field(min_length=1, max_length=HARD_BATCH_CAP)


class EventResult(ApiModel):
    index: int
    status: Literal["accepted", "duplicate", "invalid"]
    event_id: str | None = None
    errors: list[str] | None = None


class BatchResult(ApiModel):
    accepted: int
    duplicates: int
    invalid: int
    results: list[EventResult]


class StoredEvent(ApiModel):
    """What goes on the stream and, later, into a Parquet row."""

    event_id: str
    project_id: str
    name: str
    user_id: str
    timestamp: datetime
    received_at: datetime
    schema_version: int
    properties: dict[str, Any]
