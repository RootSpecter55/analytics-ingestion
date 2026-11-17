import asyncio
import time
from datetime import date, timedelta
from typing import Any
from uuid import UUID

import boto3

from app.providers.query_engine.base import DailyCount, FunnelStep, RetentionPoint


def literal(value: str | int | date) -> str:
    """Athena's ExecutionParameters are substituted as SQL literals, so strings must be quoted
    by the caller. Quotes inside a value are doubled, so a value can never end the literal."""
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


class AthenaQueryEngine:
    """Amazon Athena over the same Parquet files in S3, through a Glue table partitioned by
    project_id, date and hour (the table definition is not part of this repository).

    Values go in through ExecutionParameters (`?` placeholders); the table name comes from
    settings. The SQL mirrors the DuckDB engine in Trino syntax. Written against the official
    SDK but not exercised by the tests: it needs AWS resources.
    """

    name = "athena"

    def __init__(
        self,
        database: str,
        table: str,
        workgroup: str,
        output_location: str,
        region: str,
        client: Any = None,
    ) -> None:
        self._table = f'"{database}"."{table}"'
        self._workgroup = workgroup
        self._output = output_location
        self._client = client or boto3.client("athena", region_name=region)

    def _events(self) -> str:
        return f"""
        events AS (
            SELECT event_id, arbitrary(name) AS name, arbitrary(user_id) AS user_id,
                   arbitrary("timestamp") AS ts, CAST(arbitrary("timestamp") AS DATE) AS day
            FROM {self._table}
            WHERE project_id = ? AND date BETWEEN ? AND ?
            GROUP BY event_id
        )"""

    def _run(self, sql: str, params: list[str | int | date]) -> list[list[str]]:
        started = self._client.start_query_execution(
            QueryString=sql,
            ExecutionParameters=[literal(p) for p in params],
            WorkGroup=self._workgroup,
            ResultConfiguration={"OutputLocation": self._output},
        )
        query_id = started["QueryExecutionId"]
        deadline = time.monotonic() + 120
        while True:
            state = self._client.get_query_execution(QueryExecutionId=query_id)
            status = state["QueryExecution"]["Status"]["State"]
            if status == "SUCCEEDED":
                break
            if status in ("FAILED", "CANCELLED") or time.monotonic() > deadline:
                reason = state["QueryExecution"]["Status"].get("StateChangeReason", status)
                raise RuntimeError(f"Athena query {query_id} did not succeed: {reason}")
            time.sleep(0.5)
        rows: list[list[str]] = []
        paginator = self._client.get_paginator("get_query_results")
        for page in paginator.paginate(QueryExecutionId=query_id):
            for row in page["ResultSet"]["Rows"]:
                rows.append([cell.get("VarCharValue", "") for cell in row["Data"]])
        return rows[1:]  # the first row holds the column names

    async def _query(self, sql: str, params: list[str | int | date]) -> list[list[str]]:
        return await asyncio.to_thread(self._run, sql, params)

    async def daily_counts(
        self, project_id: UUID, event: str | None, start: date, end: date
    ) -> list[DailyCount]:
        sql = f"WITH {self._events()} SELECT day, count(*) FROM events "
        params: list[str | int | date] = [str(project_id), start.isoformat(), end.isoformat()]
        if event is not None:
            sql += "WHERE name = ? "
            params.append(event)
        rows = await self._query(sql + "GROUP BY day ORDER BY day", params)
        return [DailyCount(day=date.fromisoformat(d), events=int(c)) for d, c in rows]

    async def unique_users(
        self, project_id: UUID, event: str | None, start: date, end: date
    ) -> int:
        sql = f"WITH {self._events()} SELECT count(DISTINCT user_id) FROM events"
        params: list[str | int | date] = [str(project_id), start.isoformat(), end.isoformat()]
        if event is not None:
            sql += " WHERE name = ?"
            params.append(event)
        return int((await self._query(sql, params))[0][0])

    async def funnel(
        self, project_id: UUID, steps: list[str], start: date, end: date, window_seconds: int
    ) -> list[FunnelStep]:
        if not 2 <= len(steps) <= 5:
            raise ValueError("A funnel has 2 to 5 steps")
        params: list[str | int | date] = [str(project_id), start.isoformat(), end.isoformat()]
        ctes = [
            self._events(),
            "s1 AS (SELECT user_id, min(ts) AS t1 FROM events WHERE name = ? GROUP BY user_id)",
        ]
        params.append(steps[0])
        for n in range(2, len(steps) + 1):
            ctes.append(
                f"s{n} AS (SELECT p.user_id, p.t1, min(e.ts) AS t{n} FROM s{n - 1} p "
                f"JOIN events e ON e.user_id = p.user_id AND e.name = ? "
                f"AND e.ts >= p.t{n - 1} AND e.ts <= p.t1 + ? * INTERVAL '1' SECOND "
                f"GROUP BY p.user_id, p.t1)"
            )
            params.extend([steps[n - 1], window_seconds])
        counts = ", ".join(f"(SELECT count(*) FROM s{n})" for n in range(1, len(steps) + 1))
        row = (await self._query(f"WITH {', '.join(ctes)} SELECT {counts}", params))[0]
        return [
            FunnelStep(step=n, name=name, users=int(count))
            for n, (name, count) in enumerate(zip(steps, row, strict=True), start=1)
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
        last = cohort_end + timedelta(days=max(days))
        offsets = ", ".join("(?)" for _ in days)
        sql = f"""
        WITH {self._events()},
        cohort AS (
            SELECT user_id, min(day) AS cohort_day FROM events WHERE name = ? GROUP BY user_id
        ),
        returns AS (SELECT DISTINCT user_id, day FROM events WHERE name = ?),
        offsets (n) AS (VALUES {offsets})
        SELECT o.n, count(DISTINCT c.user_id), count(DISTINCT r.user_id)
        FROM offsets o CROSS JOIN cohort c
        LEFT JOIN returns r ON r.user_id = c.user_id AND r.day = date_add('day', o.n, c.cohort_day)
        WHERE c.cohort_day BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
        GROUP BY o.n ORDER BY o.n"""
        params: list[str | int | date] = [
            str(project_id),
            cohort_start.isoformat(),
            last.isoformat(),
            start_event,
            return_event,
            *days,
            cohort_start.isoformat(),
            cohort_end.isoformat(),
        ]
        found = {int(n): (int(c), int(r)) for n, c, r in await self._query(sql, params)}
        return [
            RetentionPoint(
                day=n, cohort_size=found.get(n, (0, 0))[0], retained=found.get(n, (0, 0))[1]
            )
            for n in days
        ]
