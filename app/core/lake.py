"""Where and how events are stored: shared by the flusher, compaction and the query engine.

    events/project_id=<uuid>/date=YYYY-MM-DD/hour=HH/part-<id>.parquet

Hive-style `key=value` directories let DuckDB and Athena read project, date and hour from the
path and skip whole directories that a query's date range excludes.
"""

import io
from datetime import datetime
from typing import Any
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq

EVENTS_PREFIX = "events"

EVENT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("project_id", pa.string()),
        ("name", pa.string()),
        ("user_id", pa.string()),
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("received_at", pa.timestamp("us", tz="UTC")),
        ("schema_version", pa.int32()),
        ("properties", pa.string()),  # JSON text: each event name has its own shape
    ]
)


def project_prefix(project_id: UUID | str) -> str:
    return f"{EVENTS_PREFIX}/project_id={project_id}"


def partition_prefix(project_id: UUID | str, timestamp: datetime) -> str:
    """The partition an event belongs to, from its own timestamp (not the time it arrived), so
    a late event lands next to the other events of that hour."""
    return f"{project_prefix(project_id)}/date={timestamp:%Y-%m-%d}/hour={timestamp:%H}"


def table_to_parquet(table: pa.Table) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    return buffer.getvalue()


def to_parquet(rows: list[dict[str, Any]]) -> bytes:
    return table_to_parquet(pa.Table.from_pylist(rows, schema=EVENT_SCHEMA))


def read_parquet(data: bytes) -> pa.Table:
    return pq.read_table(io.BytesIO(data), schema=EVENT_SCHEMA)
