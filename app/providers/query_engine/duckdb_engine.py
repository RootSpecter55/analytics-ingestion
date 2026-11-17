import asyncio
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import duckdb

from app.core.lake import project_prefix
from app.providers.query_engine.base import DailyCount, FunnelStep, RetentionPoint

# Every query starts from this: the project's events in a date range, one row per event id
# (the flusher may have written an event twice). Filtering on the hive `date` column lets DuckDB
# skip whole directories outside the range.
EVENTS_CTE = """
events AS (
    SELECT DISTINCT ON (event_id)
        event_id, name, user_id, "timestamp", CAST("timestamp" AS DATE) AS day
    FROM read_parquet($path, hive_partitioning = true, hive_types = {'date': DATE, 'hour': INTEGER})
    WHERE date BETWEEN $start AND $end
)
"""


def funnel_sql(step_count: int) -> str:
    """The funnel query for 2-5 steps. Only `step_count` (an int checked here) shapes the text;
    step names, dates and the window are bound parameters ($step1, $start, $window, ...).

    Each step keeps, per user, the earliest time they did it after the previous step, and no
    later than `window` seconds after their first step.
    """
    if not 2 <= step_count <= 5:
        raise ValueError("A funnel has 2 to 5 steps")
    ctes = [
        EVENTS_CTE,
        "s1 AS (SELECT user_id, min(timestamp) AS t1 FROM events WHERE name = $step1 "
        "GROUP BY user_id)",
    ]
    for n in range(2, step_count + 1):
        ctes.append(
            f"s{n} AS (SELECT p.user_id, p.t1, min(e.timestamp) AS t{n} FROM s{n - 1} p "
            f"JOIN events e ON e.user_id = p.user_id AND e.name = $step{n} "
            f"AND e.timestamp >= p.t{n - 1} AND e.timestamp <= p.t1 + to_seconds($window) "
            f"GROUP BY p.user_id, p.t1)"
        )
    counts = ", ".join(f"(SELECT count(*) FROM s{n})" for n in range(1, step_count + 1))
    return f"WITH {', '.join(ctes)} SELECT {counts}"


RETENTION_SQL = f"""
WITH {EVENTS_CTE},
cohort AS (
    -- Each user's first day of the start event in the queried range.
    SELECT user_id, min(day) AS cohort_day FROM events WHERE name = $start_event GROUP BY user_id
),
returns AS (SELECT DISTINCT user_id, day FROM events WHERE name = $return_event),
offsets AS (SELECT unnest($days) AS n)
SELECT o.n, count(DISTINCT c.user_id), count(DISTINCT r.user_id)
FROM offsets o
CROSS JOIN cohort c
LEFT JOIN returns r ON r.user_id = c.user_id AND r.day = c.cohort_day + o.n
WHERE c.cohort_day BETWEEN $cohort_start AND $cohort_end
GROUP BY o.n
ORDER BY o.n
"""


class DuckDbQueryEngine:
    """DuckDB, in-process, reading the Parquet files under the local data directory. A fresh
    in-memory connection per query keeps queries independent; it runs in a worker thread."""

    name = "duckdb"

    def __init__(self, data_root: str) -> None:
        self._root = Path(data_root).resolve()

    def _project_glob(self, project_id: UUID) -> str | None:
        """The project's file pattern, or None if it has no files yet (DuckDB errors on an empty
        glob). The project id is a UUID from the authenticated key, never request input."""
        directory = self._root / project_prefix(project_id)
        if not directory.exists() or not any(directory.rglob("*.parquet")):
            return None
        return str(directory / "**" / "*.parquet")

    def _run(self, sql: str, params: dict[str, Any]) -> list[tuple[Any, ...]]:
        connection = duckdb.connect()
        try:
            connection.execute("SET TimeZone = 'UTC'")
            return connection.execute(sql, params).fetchall()
        finally:
            connection.close()

    async def _query(self, sql: str, params: dict[str, Any]) -> list[tuple[Any, ...]]:
        return await asyncio.to_thread(self._run, sql, params)

    async def daily_counts(
        self, project_id: UUID, event: str | None, start: date, end: date
    ) -> list[DailyCount]:
        path = self._project_glob(project_id)
        if path is None:
            return []
        rows = await self._query(
            f"WITH {EVENTS_CTE} SELECT day, count(*) FROM events "
            "WHERE ($event IS NULL OR name = $event) GROUP BY day ORDER BY day",
            {"path": path, "start": start, "end": end, "event": event},
        )
        return [DailyCount(day=day, events=count) for day, count in rows]

    async def unique_users(
        self, project_id: UUID, event: str | None, start: date, end: date
    ) -> int:
        path = self._project_glob(project_id)
        if path is None:
            return 0
        rows = await self._query(
            f"WITH {EVENTS_CTE} SELECT count(DISTINCT user_id) FROM events "
            "WHERE ($event IS NULL OR name = $event)",
            {"path": path, "start": start, "end": end, "event": event},
        )
        return int(rows[0][0])

    async def funnel(
        self, project_id: UUID, steps: list[str], start: date, end: date, window_seconds: int
    ) -> list[FunnelStep]:
        path = self._project_glob(project_id)
        counts = [0] * len(steps)
        if path is not None:
            params: dict[str, Any] = {
                "path": path,
                "start": start,
                "end": end,
                "window": window_seconds,
            }
            params.update({f"step{n}": name for n, name in enumerate(steps, start=1)})
            counts = list((await self._query(funnel_sql(len(steps)), params))[0])
        return [
            FunnelStep(step=n, name=name, users=int(count))
            for n, (name, count) in enumerate(zip(steps, counts, strict=True), start=1)
        ]

    async def retention(
        self,
        project_id: UUID,
        start_event: str,
        return_event: str,
        cohort_start: date,
        cohort_end: date,
        days: list[int],
    ) -> list[RetentionPoint]:
        path = self._project_glob(project_id)
        if path is None:
            return [RetentionPoint(day=n, cohort_size=0, retained=0) for n in days]
        rows = await self._query(
            RETENTION_SQL,
            {
                "path": path,
                # Load enough days after the cohort for the longest offset.
                "start": cohort_start,
                "end": cohort_end + timedelta(days=max(days)),
                "start_event": start_event,
                "return_event": return_event,
                "cohort_start": cohort_start,
                "cohort_end": cohort_end,
                "days": days,
            },
        )
        found = {n: (cohort, retained) for n, cohort, retained in rows}
        return [
            RetentionPoint(
                day=n, cohort_size=found.get(n, (0, 0))[0], retained=found.get(n, (0, 0))[1]
            )
            for n in days
        ]
