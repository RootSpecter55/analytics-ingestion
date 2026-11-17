from typing import Protocol


class StorageProvider(Protocol):
    """Where Parquet files live. Implementations: local disk (default) and S3."""

    name: str

    async def put(self, key: str, data: bytes) -> None: ...

    async def get(self, key: str) -> bytes: ...

    async def list(self, prefix: str) -> list[str]:
        """Every key under `prefix`, sorted."""
        ...

    async def delete(self, key: str) -> None: ...
