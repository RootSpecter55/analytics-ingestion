"""Merges the many small Parquet files the flusher writes into one file per finished hour, and
drops duplicate event ids on the way. Safe to run repeatedly (e.g. hourly from a scheduler).

    python -m scripts.compact
"""

import asyncio

from dotenv import load_dotenv

from app.core.config import get_settings
from app.modules.ingestion.compaction import Compactor
from app.providers.storage.factory import create_storage


async def main() -> None:
    load_dotenv()  # never overrides variables that are already set
    report = await Compactor(create_storage(get_settings())).compact()
    print(
        f"Compacted {report.partitions} partition(s): {report.files_in} files -> "
        f"{report.files_out}, {report.rows_in} rows -> {report.rows_out}"
    )


if __name__ == "__main__":
    asyncio.run(main())
