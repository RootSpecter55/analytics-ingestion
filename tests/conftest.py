"""Test configuration. The environment is set before any app module reads settings."""

import os
import tempfile

os.environ.update(
    {
        "APP_ENV": "test",
        "LOG_LEVEL": "WARNING",
        "DATABASE_URL": os.environ.get(
            "TEST_DATABASE_URL", "postgresql+asyncpg://app:app@localhost:5450/analytics_test"
        ),
        "REDIS_URL": os.environ.get("TEST_REDIS_URL", "redis://localhost:6384/1"),
        "ADMIN_API_KEY": "test-admin-key-that-is-at-least-32-characters",
        "STORAGE_PROVIDER": "local",
        "LOCAL_DATA_DIR": tempfile.mkdtemp(prefix="analytics-test-lake-"),
        "QUERY_ENGINE": "duckdb",
        "EVENTS_STREAM": "events-test",
        "MAX_BATCH_SIZE": "500",
        "FLUSH_MAX_EVENTS": "1000",
        "FLUSH_MAX_SECONDS": "60",
    }
)
