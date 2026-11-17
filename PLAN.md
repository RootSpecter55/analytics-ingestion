# PLAN — analytics-ingestion

A product-analytics backend.

1. Apps send event batches with a write key.
2. Each event is validated against a versioned JSON Schema, deduplicated, and put on a Redis Stream.
3. A flusher writes the events to Parquet, partitioned by project, date and hour.
4. A query API answers daily counts, unique users, funnels and day-N retention by running DuckDB over those files.

**Stack:** Python 3.12 with uv, FastAPI, SQLAlchemy 2 (async) with asyncpg, Alembic, Pydantic v2, `jsonschema`, `redis` (asyncio), `pyarrow`, `duckdb`, boto3 (S3/Athena adapters only), pytest with pytest-asyncio, ruff, PostgreSQL 16, Redis 7.

## Modules

| Module        | Responsibility                                                                                                                                                                                                                     |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| projects      | Admin creates projects. Each gets a **write key** (ingestion) and a **read key** (queries), shown once and stored as SHA-256.                                                                                                      |
| event-schemas | Versioned JSON Schema per event name and project. Registering a schema adds version n+1; schemas are checked to be valid Draft 2020-12 object schemas. List and get versions.                                                      |
| ingestion     | `POST /v1/batch` (up to 500 events): per-event envelope and schema validation, 24 h dedup on event id, append to the stream. Returns per-event results; invalid events are reported by index. The flusher (worker) lives here too. |
| queries       | Daily event counts, unique users, 2–5 step funnel with a conversion window, day-N retention, all through `QueryEngine`.                                                                                                            |

**Shared (`app/core/`):** settings, logging, errors and handlers, pagination and envelopes, API-model base, auth dependencies (admin key, project write/read key), security, database and base repository, Redis client.

**Providers (each chosen by a factory):**

- `StorageProvider`: local disk (default) or S3 (untested). Parquet files live here.
- `QueryEngine`: DuckDB over the local Parquet files (default) or Athena (untested).

**Entrypoints:** `app.main` (API) and `app.flusher` (stream → Parquet, with `/health`), separate compose services sharing the data volume. CLIs:

- `python -m scripts.compact` merges small files per hour.
- `python -m scripts.load_generator` sends synthetic traffic.
- `python -m scripts.throughput` measures ingestion and flush.

## Data model

Postgres (metadata only):

```
projects       id, name, write_key_hash (unique), read_key_hash (unique), created_at
event_schemas  id, project_id, event_name, version, schema (json), created_at   unique(project_id, event_name, version)
```

Event data, as Parquet files in storage:

```
events/project_id=<uuid>/date=YYYY-MM-DD/hour=HH/part-<uuid>.parquet
columns: event_id, project_id, name, user_id, timestamp (UTC), received_at, schema_version, properties (JSON text)
```

## Mechanisms

- **Per-event validation.**
  - Envelope: id, name, user id, timestamp (no more than 5 min in the future, at most 365 days old), properties object.
  - Properties: the latest schema version for that event name, or an explicit `schemaVersion`.
  - An event name with no schema is invalid.
  - The batch is never all-or-nothing. The response lists `accepted`, `duplicate` or `invalid` (with errors) per index.
- **Dedup in Redis.** A small Lua script does `SET dedup:<project>:<eventId> NX EX 86400` and, only if that succeeded, `XADD` to the stream, atomically. A crash can't mark an id as seen without the event being queued. Invalid events never consume their id.
- **Flusher.**
  - Reads the stream through a consumer group and buffers events.
  - Flushes when the buffer reaches `FLUSH_MAX_EVENTS` or its oldest event reaches `FLUSH_MAX_SECONDS`.
  - Groups events by (project, date, hour) **of each event's own timestamp**, so late events land in their own partition. Writes one Parquet file per partition.
  - Acks the stream entries only after every file is written. A crash before the ack means the same events are written again (at-least-once). Queries therefore count **distinct event ids**, and compaction removes the duplicates.
- **Compaction CLI.** For each hour partition with more than one file: read all files, drop duplicate event ids, write one file, then delete the inputs.
- **Queries built from validated parameters only.** Every value (event names, dates, window, day list) is a bound parameter. The only things that shape the SQL text are the number of funnel steps (validated 2–5) and the project's directory, which comes from the authenticated key (a UUID), never from the request. _Test:_ an event name containing `' OR 1=1 --` is treated as a literal name and matches nothing.

## Endpoints

```
GET  /health
POST /v1/projects                                         [admin] returns write and read keys once
GET  /v1/projects                                         [admin]
POST /v1/projects/{id}/event-schemas                      [admin] {eventName, schema} → next version
GET  /v1/projects/{id}/event-schemas                      [admin] ?eventName=
GET  /v1/projects/{id}/event-schemas/{eventName}/versions/{version}  [admin]
POST /v1/batch                                            [write key] {events: [...] (1–500)}
GET  /v1/queries/event-counts                             [read key] ?event=&from=&to=  (daily, paginated)
GET  /v1/queries/unique-users                             [read key] ?event=&from=&to=
POST /v1/queries/funnel                                   [read key] {steps[2-5], from, to, windowSeconds}
POST /v1/queries/retention                                [read key] {startEvent, returnEvent, cohortFrom, cohortTo, days[]}
```

## Tests

- **Integration (real Postgres and Redis, Parquet in a temp dir):**
  - Mixed batch: valid, invalid by schema, malformed envelope, unknown event, duplicate within the batch, and the same id again in a second batch.
  - Schema versioning and explicit versions.
  - Flusher: size trigger, time trigger (fake clock), late event in its own partition, and a crash before ack causing re-delivery without double counting.
  - Queries against known data: daily counts, unique users, a funnel inside and outside the window, day-1 and day-7 retention.
  - The injection test.
  - Compaction merges files and drops duplicates.
  - Keys are separated: a read key can't ingest and a write key can't query.
- **Unit:** envelope validation, partition keys, schema registration rules, query parameter validation, settings, `start_with_secrets`.

## Build order

1. Scaffold. 2. Core and schema. 3. projects and event-schemas. 4. Storage provider and Redis. 5. ingestion (batch, dedup script). 6. Flusher and compaction. 7. queries (DuckDB engine, Athena adapter). 8. Tests. 9. Load generator and throughput scripts. 10. Deployment files. 11. README.
