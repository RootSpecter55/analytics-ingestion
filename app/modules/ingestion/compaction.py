import asyncio
import re
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pyarrow as pa

from app.core.lake import EVENTS_PREFIX, read_parquet, table_to_parquet
from app.providers.storage.base import StorageProvider

_PARTITION = re.compile(r"date=(\d{4}-\d{2}-\d{2})/hour=(\d{2})$")


@dataclass
class CompactionReport:
    partitions: int = 0
    files_in: int = 0
    files_out: int = 0
    rows_in: int = 0
    rows_out: int = 0


def drop_duplicate_events(table: pa.Table) -> pa.Table:
    """Keeps the first row of each event id (the flusher writes at-least-once)."""
    seen: set[str] = set()
    keep = []
    for index, event_id in enumerate(table.column("event_id").to_pylist()):
        if event_id not in seen:
            seen.add(event_id)
            keep.append(index)
    return table.take(keep)


class Compactor:
    """Merges the small files the flusher leaves in each hour into one file per hour.

    Only hours that are over (the flusher may still write into the current one) are touched.
    The merged file is written before the inputs are deleted; a query running in between may see
    rows twice, which is harmless because queries count distinct event ids.
    """

    def __init__(self, storage: StorageProvider) -> None:
        self._storage = storage

    async def compact(self, now: datetime | None = None) -> CompactionReport:
        now = now or datetime.now(UTC)
        by_partition: dict[str, list[str]] = defaultdict(list)
        for key in await self._storage.list(EVENTS_PREFIX):
            if key.endswith(".parquet"):
                by_partition[key.rsplit("/", 1)[0]].append(key)

        report = CompactionReport()
        for prefix, keys in sorted(by_partition.items()):
            match = _PARTITION.search(prefix)
            if len(keys) < 2 or match is None:
                continue
            hour_start = datetime.fromisoformat(f"{match[1]}T{match[2]}:00:00+00:00")
            if hour_start + timedelta(hours=1) > now:
                continue  # the hour is not over yet

            tables = [read_parquet(await self._storage.get(key)) for key in keys]
            merged = drop_duplicate_events(pa.concat_tables(tables))
            output = f"{prefix}/part-compacted-{uuid.uuid4().hex}.parquet"
            await self._storage.put(output, await asyncio.to_thread(table_to_parquet, merged))
            for key in keys:
                await self._storage.delete(key)

            report.partitions += 1
            report.files_in += len(keys)
            report.files_out += 1
            report.rows_in += sum(t.num_rows for t in tables)
            report.rows_out += merged.num_rows
        return report
