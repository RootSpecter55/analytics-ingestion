"""Per-event validation: pure functions, so they are easy to test and reason about."""

from datetime import UTC, datetime, timedelta
from typing import Any

from jsonschema import Draft202012Validator
from pydantic import ValidationError

from app.modules.event_schemas.models import EventSchema
from app.modules.ingestion.schemas import EventIn

MAX_ERRORS_PER_EVENT = 10


def check_envelope(
    raw: dict[str, Any], now: datetime, max_future: timedelta, max_age: timedelta
) -> tuple[EventIn | None, list[str]]:
    """Parses the envelope. Returns (event, []) or (None, errors)."""
    try:
        event = EventIn.model_validate(raw)
    except ValidationError as exc:
        errors = [
            f"{'.'.join(str(p) for p in e['loc']) or 'event'}: {e['msg']}" for e in exc.errors()
        ]
        return None, errors[:MAX_ERRORS_PER_EVENT]
    timestamp = event.timestamp.astimezone(UTC)
    if timestamp > now + max_future:
        return None, ["timestamp: is in the future"]
    if timestamp < now - max_age:
        return None, [f"timestamp: is older than {max_age.days} days"]
    return event, []


class SchemaBook:
    """The schemas a batch needs, loaded once: every version of every event name in it."""

    def __init__(self, rows: list[EventSchema]) -> None:
        self._versions: dict[str, dict[int, dict[str, Any]]] = {}
        for row in rows:
            self._versions.setdefault(row.event_name, {})[row.version] = row.schema

    def check(self, event: EventIn) -> tuple[int | None, list[str]]:
        """Validates the properties. Returns (schema version used, errors)."""
        versions = self._versions.get(event.name)
        if not versions:
            return None, [f"name: no schema is registered for event '{event.name}'"]
        version = event.schema_version or max(versions)
        schema = versions.get(version)
        if schema is None:
            return None, [f"schemaVersion: '{event.name}' has no version {version}"]
        errors = sorted(
            Draft202012Validator(schema).iter_errors(event.properties), key=lambda e: e.json_path
        )
        messages = [f"properties{e.json_path.removeprefix('$')}: {e.message}" for e in errors]
        return version, messages[:MAX_ERRORS_PER_EVENT]
