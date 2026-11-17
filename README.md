# analytics-ingestion

A product-analytics backend. Apps send events in batches. Each event is checked against a **versioned JSON Schema** and **deduplicated for 24 hours**, then queued on a Redis Stream. A **flusher** writes the events to **Parquet files partitioned by project, date and hour**. A query API answers **daily counts, unique users, funnels and day-N retention** by running DuckDB over those files.

The interesting parts:

- **Per-event validation.** One bad event never sinks the batch.
- **Atomic dedup.** A Lua script both remembers the event id and queues the event, so it can't do one without the other.
- **Late events.** They land in the partition of their own timestamp.
- **At-least-once flushing that never double-counts.**
- **Queries built only from validated, bound parameters.**

**What runs where:** everything runs locally with Docker, with no cloud account. Parquet files go to a local directory and DuckDB queries them in-process. **S3** storage and an **Athena** query engine sit behind the same interfaces, written against boto3, but are not exercised by the tests. The project is **not deployed**.

**Stack:** Python 3.12, uv, FastAPI, SQLAlchemy 2 (async), Alembic, Pydantic v2, `jsonschema`, `redis` (asyncio), `pyarrow`, `duckdb`, pytest, ruff, PostgreSQL 16 (projects and schemas only), Redis 7 (dedup and the stream).

## Architecture

```mermaid
flowchart LR
  App[Your app] -->|"POST /v1/batch<br/>write key"| API
  subgraph API["API process"]
    V["validate each event<br/>(envelope + JSON Schema)"]
    Q[queries]
  end
  V -->|"schemas"| PG[(PostgreSQL)]
  V -->|"Lua: SET NX EX 24h<br/>+ XADD, atomically"| RS[(Redis Stream)]
  RS -->|"consumer group"| F["Flusher<br/>(same codebase)"]
  F -->|"one Parquet file per<br/>project/date/hour,<br/>then XACK"| ST{{StorageProvider}}
  ST --> LD[local disk]
  ST -.-> S3[S3]
  Dash[Dashboard] -->|"read key"| Q
  Q --> QE{{QueryEngine}}
  QE --> DK["DuckDB over<br/>local Parquet"]
  QE -.-> AT[Athena]
  C["compact CLI"] -->|"merge small files<br/>per finished hour"| ST
```

Dashed lines are the AWS adapters, selected with `STORAGE_PROVIDER=s3` and `QUERY_ENGINE=athena`.

```
app/
  core/        settings, logging, errors, pagination/envelopes, ApiModel, auth (admin key, project
               write/read keys), security, db, base repository, redis, lake (Parquet layout + schema)
  providers/   storage/ (local, S3, factory)    query_engine/ (DuckDB, Athena, factory)
  modules/     projects, event_schemas, ingestion (+ validation, sink, flusher, compaction), queries
  container.py composition root     main.py  API     flusher.py  flusher entrypoint
scripts/       start_with_secrets.py, compact.py, load_generator.py, throughput.py
tests/         unit/, integration/ (real Postgres + Redis, Parquet in a temp directory)
```

Each module has `routes → controller → service → repository` plus Pydantic `schemas`. Validation (`ingestion/validation.py`) is pure functions, so it is unit-tested on its own.

## Data model

```mermaid
erDiagram
  projects ||--o{ event_schemas : defines
  projects {
    uuid id PK
    string name
    string write_key_hash UK "sha256"
    string read_key_hash UK "sha256"
  }
  event_schemas {
    uuid id PK
    uuid project_id FK
    string event_name
    int version "unique per project + name"
    json schema "JSON Schema 2020-12 for properties"
  }
```

Events are not in Postgres. They live in Parquet:

```
events/project_id=<uuid>/date=YYYY-MM-DD/hour=HH/part-<id>.parquet

event_id  project_id  name  user_id  timestamp (UTC)  received_at  schema_version  properties (JSON text)
```

## Run it

Requires Docker.

```bash
cp .env.example .env
docker compose up --build
```

This starts Postgres (**5450**) and Redis (**6384**, append-only, so queued events survive a restart), runs migrations, then starts the **api** on <http://localhost:8000> (docs at `/docs`) and the **flusher**. They share the data volume.

```bash
J='Content-Type: application/json'; ADMIN='Authorization: Bearer change-me-admin-key-at-least-32-characters'
P=$(curl -s localhost:8000/v1/projects -H "$ADMIN" -H "$J" -d '{"name":"Shop"}')
PID=$(echo $P | jq -r .data.project.id)
W="Authorization: Bearer $(echo $P | jq -r .data.writeKey)"; R="Authorization: Bearer $(echo $P | jq -r .data.readKey)"

curl -s localhost:8000/v1/projects/$PID/event-schemas -H "$ADMIN" -H "$J" \
  -d '{"eventName":"signup","schema":{"type":"object","properties":{"plan":{"enum":["free","pro"]}},"required":["plan"]}}'

curl -s localhost:8000/v1/batch -H "$W" -H "$J" -d "{\"events\":[
  {\"eventId\":\"a1\",\"name\":\"signup\",\"userId\":\"u1\",\"timestamp\":\"$(date -u +%FT%TZ)\",\"properties\":{\"plan\":\"pro\"}},
  {\"eventId\":\"a2\",\"name\":\"signup\",\"userId\":\"u2\",\"timestamp\":\"$(date -u +%FT%TZ)\",\"properties\":{\"plan\":\"gold\"}}]}"
# → accepted 1, invalid 1 with "properties.plan: 'gold' is not one of ['free', 'pro']"

# Within FLUSH_MAX_SECONDS (60 s by default) the event is in Parquet and queryable:
curl -s "localhost:8000/v1/queries/unique-users?event=signup&from=$(date -u +%F)&to=$(date -u +%F)" -H "$R"
curl -s localhost:8000/v1/queries/funnel -H "$R" -H "$J" \
  -d "{\"steps\":[\"signup\",\"checkout\"],\"from\":\"$(date -u +%F)\",\"to\":\"$(date -u +%F)\",\"windowSeconds\":3600}"
```

Synthetic traffic: `uv run python -m scripts.load_generator` (it creates a demo project and prints its read key). Compaction: `docker compose run --rm flusher python -m scripts.compact`.

### Tests and lint

```bash
docker compose up -d postgres redis   # tests use the analytics_test database and Redis db 1
uv sync
uv run ruff check . && uv run ruff format --check .
uv run pytest
```

## Endpoints

Every response uses one envelope: `{ "data": … }`, plus `"meta": { page, pageSize, total, totalPages }` on lists. Errors are `{ "error": { "code", "message", "details"? } }`. Everything is validated by Pydantic. **admin** = `ADMIN_API_KEY`, **write**/**read** = the project's keys. A read key can't send events, and a write key can't run queries.

| Method | Path                                                        | Access | Notes                                                          |
| ------ | ----------------------------------------------------------- | ------ | -------------------------------------------------------------- |
| GET    | `/health`                                                   | public |                                                                |
| POST   | `/v1/projects` · GET `/v1/projects`                         | admin  | Create returns the write and read keys once                    |
| POST   | `/v1/projects/{id}/event-schemas`                           | admin  | `{eventName, schema}` → the next version                       |
| GET    | `/v1/projects/{id}/event-schemas`                           | admin  | `?eventName=`                                                  |
| GET    | `/v1/projects/{id}/event-schemas/{name}/versions/{version}` | admin  |                                                                |
| POST   | `/v1/batch`                                                 | write  | `{events: [...]}`, 1–500; per-index results                    |
| GET    | `/v1/queries/event-counts`                                  | read   | `?event=&from=&to=` (≤ 366 days); every day, paginated         |
| GET    | `/v1/queries/unique-users`                                  | read   | `?event=&from=&to=`                                            |
| POST   | `/v1/queries/funnel`                                        | read   | `{steps (2–5), from, to, windowSeconds}`                       |
| POST   | `/v1/queries/retention`                                     | read   | `{startEvent, returnEvent, cohortFrom, cohortTo, days (≤ 10)}` |

An event is `{eventId, name, userId, timestamp (with time zone), properties?, schemaVersion?}`. Its timestamp may be at most 5 minutes in the future and 365 days in the past.

## Key design decisions

**Per-event results, never all-or-nothing.** A mobile SDK that retries a whole batch because one event was bad would resend the good ones forever. So every event is parsed and schema-checked on its own, and the response says, per index, `accepted`, `duplicate` or `invalid`, with the errors. One query loads every schema version the batch needs. An event uses the latest version of its schema unless it pins `schemaVersion`. Versions are append-only, so old events stay explainable.

**Dedup and enqueue in one atomic step.** `ingestion/sink.py` runs this Lua script per event, pipelined, so it is one round trip per batch:

```lua
if redis.call('SET', 'dedup:<project>:<eventId>', '1', 'NX', 'EX', 86400) then
  redis.call('XADD', <stream>, '*', 'event', <json>)
  return 1
end
return 0
```

Done as two separate commands, a crash in between would mark an id as seen without queuing the event, and the client's retry would then be dropped as a "duplicate": silent data loss. Invalid events never reach the script, so they don't use up their id. A duplicate inside the same batch is caught too, because the scripts run in order.

**The flusher: big files, bounded delay, at-least-once.** It reads the stream through a consumer group and flushes when either:

- it holds `FLUSH_MAX_EVENTS` events (big files under load), or
- its oldest event has waited `FLUSH_MAX_SECONDS` (a bounded delay when traffic is quiet).

Each flush writes one file per **(project, date, hour) of each event's own timestamp**, so an event that arrives two days late goes into the hour it happened. Files are written to a temporary name and renamed, so readers never see half a file.

Only after every file is written are the entries acknowledged (`XACK` + `XDEL`). If the process dies in between, a restarted flusher with the same name reads its unacknowledged entries again and writes them again. Rather than make that exactly-once (hard), the read side tolerates it: **every query counts distinct event ids**, and compaction drops the duplicates. `tests/integration/test_flusher.py` kills a flusher between write and ack and checks the count stays right.

That test caught two real bugs in the backlog cursor: reading the backlog from id `0` repeatedly returned the same entries, and an empty backlog made the first poll return nothing.

**Compaction.** Under load, the flusher leaves many small files per hour, which slows queries. `scripts/compact.py` merges every finished hour (never the current one) into one file, dropping duplicate event ids, and writes the new file before deleting the old ones.

**Queries are built from validated parameters only.** Every value reaches DuckDB as a bound parameter (`$event`, `$start`, `$step1`, `$window`, `$days`, even the file path). Only two things shape the SQL text:

- the **number** of funnel steps, an integer checked to be 2–5;
- the project's directory, which comes from the authenticated read key (a UUID), never from the request.

`TestSafety` calls the engine directly, skipping the API's validation, with names like `x'; DROP TABLE events; --`. They match nothing and change nothing, and the real data still answers normally. The API also refuses anything that isn't an event name. The Athena adapter uses `ExecutionParameters`, which Athena substitutes as literals, so it quotes them and doubles any quote inside.

**What the queries mean:**

- **Event counts:** every day in the range, with zeros, counting distinct event ids.
- **Funnel:** a user's first step-1 event in the range starts their window. Each later step must happen at or after the previous one, and within `windowSeconds` of step 1.
- **Retention:** the cohort is the users whose first start event in the range falls between `cohortFrom` and `cohortTo`. A user is retained on day N if they did the return event exactly N days after their cohort day.
- **Dates:** all in UTC.

**Patterns, only where they earn their place.**

- _Singleton:_ settings, the database engine and the Redis client.
- _Factory:_ `StorageProvider` (local or S3) and `QueryEngine` (DuckDB or Athena).
- _Repository:_ projects and schemas.

There's no state-machine table: nothing here has a lifecycle beyond "queued, then written".

## Results

Not yet measured, run `uv run python -m scripts.throughput`.

## Known limitations

- **One flusher.** It uses a fixed consumer name so a restart can pick up its own backlog. Running several would need distinct names and claiming of a dead consumer's entries (`XAUTOCLAIM`), which is not implemented.
- **The stream has no length cap.** If the flusher is down, events queue in Redis memory until it comes back.
- **Dedup is a 24-hour window, keyed by event id.** A retry after that is accepted again.
- **"First" means first within the queried range.** A user who signed up before `cohortFrom` counts as new if their first signup in the range falls inside the cohort. Funnels likewise start from each user's first step-1 event in the range.
- **The DuckDB engine reads local files only.** With S3 storage, use the Athena engine.
- **The Athena adapter and S3 storage are not tested.** The Glue table definition (columns, and partition projection on project_id/date/hour) is not part of this repository.
- **Property values are stored as JSON text.** Queries can't filter on properties yet.
- **Compaction must be scheduled externally,** and it reads a whole hour into memory.

## Deployment

| File                                      | Purpose                                                                                                                                                                                                                                                                                          |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `Dockerfile`                              | Multi-stage build with uv: locked production dependencies in a virtualenv, then a slim runtime image running as the non-root `app` user, with a healthcheck on `/health`. The same image runs the API and, with the `flusher` argument, the flusher.                                             |
| `scripts/start_with_secrets.py`           | Container entrypoint. With `SECRETS_SOURCE=aws` it loads one JSON secret (`SECRET_ID`) from AWS Secrets Manager into the environment (it must contain `DATABASE_URL`, `REDIS_URL`, `ADMIN_API_KEY`); with `local` it uses `.env`. It validates the settings, then starts the API or the flusher. |
| `deploy/ecs-task-definition.json`         | ECS Fargate task definition template for the API, with `${…}` placeholders for account id, region, image, role ARNs, secret id, bucket and Athena settings. It selects `STORAGE_PROVIDER=s3` and `QUERY_ENGINE=athena`.                                                                          |
| `deploy/ecs-task-definition.flusher.json` | The same for the flusher, as a separate service that runs one task.                                                                                                                                                                                                                              |
| `.github/workflows/deploy.yml`            | Manual-only (`workflow_dispatch`) workflow. It assumes an AWS role through GitHub OIDC (no stored keys), builds and pushes the image to ECR, fills in both task definitions with `envsubst` and updates both ECS services.                                                                       |
| `.github/workflows/ci.yml`                | Runs ruff (lint and format check) and pytest against Postgres and Redis service containers on every push and pull request.                                                                                                                                                                       |

Deployment config is provided as a reference. This project has not been deployed.
