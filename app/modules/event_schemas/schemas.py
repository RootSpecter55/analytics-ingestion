from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import Field

from app.core.schemas import ApiModel

# Event names: lowercase words joined by _ . or - (e.g. "checkout_completed").
EVENT_NAME_PATTERN = r"^[a-z][a-z0-9_.-]{0,99}$"


class RegisterSchemaRequest(ApiModel):
    event_name: str = Field(pattern=EVENT_NAME_PATTERN)
    # A JSON Schema (Draft 2020-12) for the event's `properties` object.
    schema_: dict[str, Any] = Field(alias="schema")


class EventSchemaOut(ApiModel):
    id: UUID
    event_name: str
    version: int
    schema_: dict[str, Any] = Field(alias="schema", validation_alias="schema")
    created_at: datetime
