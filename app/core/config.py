from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Every setting comes from environment variables and is validated at startup."""

    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

    app_env: str = "development"
    log_level: str = "INFO"

    database_url: str = Field(pattern=r"^postgresql\+asyncpg://")
    redis_url: str = Field(pattern=r"^rediss?://")
    admin_api_key: str = Field(min_length=32)

    # Event storage
    storage_provider: Literal["local", "s3"] = "local"
    local_data_dir: str = "./data/lake"
    s3_bucket: str | None = None
    aws_region: str | None = None

    # Queries
    query_engine: Literal["duckdb", "athena"] = "duckdb"
    athena_database: str | None = Field(default=None, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    athena_table: str = Field(default="events", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    athena_workgroup: str = "primary"
    athena_output_location: str | None = None

    # Ingestion
    max_batch_size: int = Field(default=500, ge=1, le=5000)
    dedup_ttl_seconds: int = Field(default=86_400, gt=0)
    max_future_seconds: int = Field(default=300, ge=0)
    max_event_age_days: int = Field(default=365, gt=0)
    events_stream: str = Field(default="events", min_length=1)

    # Flusher
    flush_max_events: int = Field(default=5000, gt=0)
    flush_max_seconds: float = Field(default=60, gt=0)
    flusher_name: str = Field(default="flusher-1", min_length=1)

    @field_validator(
        "s3_bucket", "aws_region", "athena_database", "athena_output_location", mode="before"
    )
    @classmethod
    def empty_means_unset(cls, value: object) -> object:
        """`ATHENA_DATABASE=` in a .env file means "not set", not an empty name."""
        return None if value == "" else value

    @model_validator(mode="after")
    def check_provider_settings(self) -> Self:
        problems = []
        if self.storage_provider == "s3" and not (self.s3_bucket and self.aws_region):
            problems.append("STORAGE_PROVIDER=s3 needs S3_BUCKET and AWS_REGION")
        if self.query_engine == "duckdb" and self.storage_provider != "local":
            problems.append("QUERY_ENGINE=duckdb reads local files: use STORAGE_PROVIDER=local")
        if self.query_engine == "athena" and not (
            self.athena_database and self.athena_output_location and self.aws_region
        ):
            problems.append(
                "QUERY_ENGINE=athena needs ATHENA_DATABASE, ATHENA_OUTPUT_LOCATION and AWS_REGION"
            )
        if problems:
            raise ValueError("; ".join(problems))
        return self


@lru_cache
def get_settings() -> Settings:
    """Singleton: settings are parsed and validated once, then shared."""
    return Settings()  # type: ignore[call-arg]  # values come from the environment
