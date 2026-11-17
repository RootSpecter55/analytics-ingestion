from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.auth import ProjectContext
from app.core.db import Database
from app.core.errors import AppError
from app.modules.event_schemas.repository import EventSchemaRepository
from app.modules.ingestion.schemas import BatchResult, EventIn, EventResult, StoredEvent
from app.modules.ingestion.sink import EventSink
from app.modules.ingestion.validation import SchemaBook, check_envelope


class IngestionService:
    def __init__(
        self,
        db: Database,
        schemas: EventSchemaRepository,
        sink: EventSink,
        *,
        max_batch_size: int,
        max_future_seconds: int,
        max_event_age_days: int,
    ) -> None:
        self._db = db
        self._schemas = schemas
        self._sink = sink
        self._max_batch = max_batch_size
        self._max_future = timedelta(seconds=max_future_seconds)
        self._max_age = timedelta(days=max_event_age_days)

    async def ingest(
        self, project: ProjectContext, raw_events: list[dict[str, Any]]
    ) -> BatchResult:
        """Validates every event on its own and accepts the valid ones. The batch is never
        all-or-nothing: the result says, per index, accepted, duplicate or invalid (and why)."""
        if len(raw_events) > self._max_batch:
            raise AppError(
                413, "BATCH_TOO_LARGE", f"A batch may contain at most {self._max_batch} events"
            )
        now = datetime.now(UTC)
        results: dict[int, EventResult] = {}

        envelopes: list[tuple[int, EventIn]] = []
        for index, raw in enumerate(raw_events):
            event, errors = check_envelope(raw, now, self._max_future, self._max_age)
            if event is None:
                event_id = raw.get("eventId") if isinstance(raw.get("eventId"), str) else None
                results[index] = EventResult(
                    index=index, status="invalid", event_id=event_id, errors=errors
                )
            else:
                envelopes.append((index, event))

        async with self._db.transaction() as session:
            book = SchemaBook(
                await self._schemas.for_names(
                    session, project.project_id, {event.name for _, event in envelopes}
                )
            )

        to_append: list[tuple[int, str, str]] = []
        for index, event in envelopes:
            version, errors = book.check(event)
            if errors or version is None:
                results[index] = EventResult(
                    index=index, status="invalid", event_id=event.event_id, errors=errors
                )
                continue
            stored = StoredEvent(
                event_id=event.event_id,
                project_id=str(project.project_id),
                name=event.name,
                user_id=event.user_id,
                timestamp=event.timestamp.astimezone(UTC),
                received_at=now,
                schema_version=version,
                properties=event.properties,
            )
            to_append.append((index, event.event_id, stored.model_dump_json()))

        added = await self._sink.append(
            str(project.project_id), [(event_id, payload) for _, event_id, payload in to_append]
        )
        for (index, event_id, _), was_added in zip(to_append, added, strict=True):
            results[index] = EventResult(
                index=index, status="accepted" if was_added else "duplicate", event_id=event_id
            )

        ordered = [results[i] for i in range(len(raw_events))]
        return BatchResult(
            accepted=sum(r.status == "accepted" for r in ordered),
            duplicates=sum(r.status == "duplicate" for r in ordered),
            invalid=sum(r.status == "invalid" for r in ordered),
            results=ordered,
        )
