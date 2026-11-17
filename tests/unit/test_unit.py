import json
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from botocore.exceptions import ClientError
from pydantic import ValidationError

from app.core.config import Settings
from app.core.errors import AppError
from app.core.lake import EVENT_SCHEMA, partition_prefix
from app.modules.event_schemas.models import EventSchema
from app.modules.event_schemas.service import check_schema
from app.modules.ingestion.compaction import drop_duplicate_events
from app.modules.ingestion.schemas import EventIn
from app.modules.ingestion.validation import SchemaBook, check_envelope
from app.modules.queries.schemas import FunnelRequest, RetentionRequest
from app.providers.query_engine.athena_engine import literal
from app.providers.query_engine.duckdb_engine import funnel_sql
from scripts.start_with_secrets import SecretsError, load_secrets, parse_role

NOW = datetime(2026, 11, 17, 12, 0, tzinfo=UTC)
FUTURE, AGE = timedelta(minutes=5), timedelta(days=365)


def raw(**overrides: Any) -> dict[str, Any]:
    return {
        "eventId": "e1",
        "name": "signup",
        "userId": "u1",
        "timestamp": "2026-11-17T11:00:00Z",
        "properties": {"plan": "pro"},
    } | overrides


class TestEnvelope:
    def test_accepts_a_complete_event(self) -> None:
        event, errors = check_envelope(raw(), NOW, FUTURE, AGE)
        assert errors == [] and event is not None and event.user_id == "u1"

    @pytest.mark.parametrize(
        ("overrides", "message"),
        [
            ({"timestamp": "2026-11-17T11:00:00"}, "timestamp: Input should have timezone info"),
            ({"timestamp": "2026-11-17T12:06:00Z"}, "timestamp: is in the future"),
            ({"timestamp": "2025-01-01T00:00:00Z"}, "timestamp: is older than 365 days"),
            ({"name": "Signup!"}, "name: String should match pattern"),
            ({"eventId": "has spaces"}, "eventId: String should match pattern"),
            ({"properties": [1, 2]}, "properties: Input should be a valid dictionary"),
            ({"surprise": 1}, "surprise: Extra inputs are not permitted"),
        ],
    )
    def test_reports_what_is_wrong(self, overrides: dict[str, Any], message: str) -> None:
        event, errors = check_envelope(raw(**overrides), NOW, FUTURE, AGE)
        assert event is None
        assert any(error.startswith(message) for error in errors), errors

    def test_converts_timestamps_to_utc_later_but_keeps_the_instant(self) -> None:
        event, _ = check_envelope(raw(timestamp="2026-11-17T13:00:00+02:00"), NOW, FUTURE, AGE)
        assert event is not None and event.timestamp.astimezone(UTC).hour == 11


class TestSchemaBook:
    BOOK = SchemaBook(
        [
            EventSchema(event_name="signup", version=1, schema={"type": "object"}),
            EventSchema(
                event_name="signup",
                version=2,
                schema={
                    "type": "object",
                    "required": ["plan"],
                    "additionalProperties": False,
                    "properties": {"plan": {"type": "string"}},
                },
            ),
        ]
    )

    def _event(self, **overrides: Any) -> EventIn:
        return EventIn.model_validate(raw(**overrides))

    def test_uses_the_latest_version_by_default(self) -> None:
        assert self.BOOK.check(self._event()) == (2, [])
        version, errors = self.BOOK.check(self._event(properties={"plan": 1, "x": 2}))
        assert version == 2
        assert errors == [
            "properties: Additional properties are not allowed ('x' was unexpected)",
            "properties.plan: 1 is not of type 'string'",
        ]

    def test_a_pinned_version_or_an_unknown_event(self) -> None:
        assert self.BOOK.check(self._event(properties={}, schemaVersion=1)) == (1, [])
        assert self.BOOK.check(self._event(name="other"))[1] == [
            "name: no schema is registered for event 'other'"
        ]

    def test_schemas_must_be_valid_object_schemas(self) -> None:
        check_schema({"type": "object", "properties": {"a": {"type": "integer"}}})
        for bad in ({"type": "wat"}, {"type": "array"}, {"properties": {}}):
            with pytest.raises(AppError):
                check_schema(bad)


class TestLakeAndCompaction:
    def test_partition_is_the_events_own_hour(self) -> None:
        assert (
            partition_prefix("p1", datetime(2026, 3, 9, 7, 45, tzinfo=UTC))
            == "events/project_id=p1/date=2026-03-09/hour=07"
        )

    def test_compaction_keeps_one_row_per_event_id(self) -> None:
        rows = [
            {
                "event_id": eid,
                "project_id": "p",
                "name": "x",
                "user_id": "u",
                "timestamp": NOW,
                "received_at": NOW,
                "schema_version": 1,
                "properties": "{}",
            }
            for eid in ["a", "b", "a", "c", "b"]
        ]
        table = drop_duplicate_events(pa.Table.from_pylist(rows, schema=EVENT_SCHEMA))
        assert table.column("event_id").to_pylist() == ["a", "b", "c"]


class TestQuerySafety:
    def test_funnel_sql_has_placeholders_not_names(self) -> None:
        sql = funnel_sql(3)
        assert "$step1" in sql and "$step3" in sql and "$step4" not in sql
        assert "$window" in sql and "'" not in sql.replace("'date'", "").replace("'hour'", "")
        for bad in (1, 6):
            with pytest.raises(ValueError, match="2 to 5 steps"):
                funnel_sql(bad)

    def test_athena_literals_cannot_end_the_string(self) -> None:
        assert literal("it's") == "'it''s'"
        assert literal("x' OR '1'='1") == "'x'' OR ''1''=''1'"
        assert literal(7) == "7"

    def test_query_requests_are_validated(self) -> None:
        request = RetentionRequest.model_validate(
            {
                "startEvent": "a",
                "returnEvent": "b",
                "cohortFrom": "2026-01-01",
                "cohortTo": "2026-01-07",
                "days": [30, 1, 7, 1],
            }
        )
        assert request.days == [1, 7, 30]
        with pytest.raises(ValidationError, match="between 1 and 90"):
            RetentionRequest.model_validate(
                {
                    "startEvent": "a",
                    "returnEvent": "b",
                    "cohortFrom": "2026-01-01",
                    "cohortTo": "2026-01-01",
                    "days": [0],
                }
            )
        with pytest.raises(ValidationError, match="at most 366 days"):
            FunnelRequest.model_validate(
                {"steps": ["a", "b"], "from": "2025-01-01", "to": "2026-06-01", "windowSeconds": 60}
            )


class TestSettings:
    BASE: ClassVar[dict[str, Any]] = {
        "database_url": "postgresql+asyncpg://u:p@localhost/db",
        "redis_url": "redis://localhost:6379/0",
        "admin_api_key": "x" * 32,
    }

    def test_empty_optional_values_mean_unset(self) -> None:
        settings = Settings(**self.BASE, athena_database="", s3_bucket="", aws_region="")
        assert (settings.athena_database, settings.s3_bucket, settings.aws_region) == (
            None,
            None,
            None,
        )

    def test_duckdb_needs_local_files_and_athena_needs_its_settings(self) -> None:
        with pytest.raises(ValidationError, match="QUERY_ENGINE=duckdb reads local files"):
            Settings(**self.BASE, storage_provider="s3", s3_bucket="b", aws_region="eu-west-1")
        with pytest.raises(ValidationError, match="QUERY_ENGINE=athena needs ATHENA_DATABASE"):
            Settings(**self.BASE, query_engine="athena")
        with pytest.raises(ValidationError, match="athena_table"):
            Settings(**self.BASE, athena_table="events; DROP")


def _client_returning(secret: Any) -> MagicMock:
    client = MagicMock()
    client.get_secret_value.return_value = {"SecretString": secret}
    return client


class TestStartWithSecrets:
    def test_copies_every_key_of_the_aws_secret(self) -> None:
        env = {"SECRETS_SOURCE": "aws", "SECRET_ID": "app/prod"}
        secret = {"DATABASE_URL": "d", "REDIS_URL": "r", "ADMIN_API_KEY": "a", "EXTRA": 1}
        load_secrets(env, _client_returning(json.dumps(secret)))
        assert env["EXTRA"] == "1" and env["REDIS_URL"] == "r"

    def test_fails_fast(self) -> None:
        with pytest.raises(SecretsError, match="SECRET_ID is required"):
            load_secrets({"SECRETS_SOURCE": "aws"}, _client_returning("{}"))
        failing = MagicMock()
        failing.get_secret_value.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "no"}}, "GetSecretValue"
        )
        with pytest.raises(SecretsError, match='Could not read secret "x"'):
            load_secrets({"SECRETS_SOURCE": "aws", "SECRET_ID": "x"}, failing)
        with pytest.raises(SecretsError, match="missing required keys: REDIS_URL, ADMIN_API_KEY"):
            load_secrets(
                {"SECRETS_SOURCE": "aws", "SECRET_ID": "x"},
                _client_returning(json.dumps({"DATABASE_URL": "d"})),
            )

    def test_roles(self) -> None:
        assert parse_role(["p"]) == "api"
        assert parse_role(["p", "flusher"]) == "flusher"
        with pytest.raises(SecretsError, match='got "worker"'):
            parse_role(["p", "worker"])
