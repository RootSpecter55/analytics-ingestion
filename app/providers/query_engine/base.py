from dataclasses import dataclass
from datetime import date
from typing import Protocol
from uuid import UUID


@dataclass(frozen=True)
class DailyCount:
    day: date
    events: int


@dataclass(frozen=True)
class FunnelStep:
    step: int
    name: str
    users: int


@dataclass(frozen=True)
class RetentionPoint:
    day: int
    cohort_size: int
    retained: int


class QueryEngine(Protocol):
    """Answers the analytics questions over the stored events. Implementations: DuckDB over
    local Parquet files (default) and Athena.

    Every method receives values that were already validated, and passes them to the database
    as bound parameters: none is ever pasted into SQL text. Duplicate deliveries are ignored by
    counting each event id once.
    """

    name: str

    async def daily_counts(
        self, project_id: UUID, event: str | None, start: date, end: date
    ) -> list[DailyCount]:
        """One row per day in [start, end] that has events."""
        ...

    async def unique_users(
        self, project_id: UUID, event: str | None, start: date, end: date
    ) -> int: ...

    async def funnel(
        self, project_id: UUID, steps: list[str], start: date, end: date, window_seconds: int
    ) -> list[FunnelStep]: ...

    async def retention(
        self,
        project_id: UUID,
        start_event: str,
        return_event: str,
        cohort_start: date,
        cohort_end: date,
        days: list[int],
    ) -> list[RetentionPoint]: ...
