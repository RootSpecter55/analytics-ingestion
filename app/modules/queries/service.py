from datetime import date, timedelta

from app.core.auth import ProjectContext
from app.core.pagination import PageParams
from app.modules.queries.schemas import (
    DailyCountOut,
    FunnelRequest,
    FunnelStepOut,
    RetentionPointOut,
    RetentionRequest,
    UniqueUsersOut,
)
from app.providers.query_engine.base import QueryEngine


class QueriesService:
    """The project always comes from the read key, so one project can never query another's
    events. Everything else is validated before it reaches the engine."""

    def __init__(self, engine: QueryEngine) -> None:
        self._engine = engine

    async def event_counts(
        self, project: ProjectContext, event: str | None, start: date, end: date, page: PageParams
    ) -> tuple[list[DailyCountOut], int]:
        """Every day in the range, including days with no events (0), one page at a time."""
        counts = {
            c.day: c.events
            for c in await self._engine.daily_counts(project.project_id, event, start, end)
        }
        days = [start + timedelta(days=n) for n in range((end - start).days + 1)]
        rows = [DailyCountOut(date=day, events=counts.get(day, 0)) for day in days]
        return rows[page.offset : page.offset + page.page_size], len(rows)

    async def unique_users(
        self, project: ProjectContext, event: str | None, start: date, end: date
    ) -> UniqueUsersOut:
        users = await self._engine.unique_users(project.project_id, event, start, end)
        return UniqueUsersOut(from_=start, to=end, event=event, users=users)

    async def funnel(self, project: ProjectContext, request: FunnelRequest) -> list[FunnelStepOut]:
        steps = await self._engine.funnel(
            project.project_id, request.steps, request.from_, request.to, request.window_seconds
        )
        first = steps[0].users
        return [
            FunnelStepOut(
                step=s.step,
                name=s.name,
                users=s.users,
                conversion=round(s.users / first, 4) if first else 0.0,
            )
            for s in steps
        ]

    async def retention(
        self, project: ProjectContext, request: RetentionRequest
    ) -> list[RetentionPointOut]:
        points = await self._engine.retention(
            project.project_id,
            request.start_event,
            request.return_event,
            request.cohort_from,
            request.cohort_to,
            request.days,
        )
        return [
            RetentionPointOut(
                day=p.day,
                cohort_size=p.cohort_size,
                retained=p.retained,
                rate=round(p.retained / p.cohort_size, 4) if p.cohort_size else 0.0,
            )
            for p in points
        ]
