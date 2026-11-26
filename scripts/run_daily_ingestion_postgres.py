
import logging
import sys

from src.youtube_intelligence_platform.api.youtube_client import (
    YouTubeClient,
)
from src.youtube_intelligence_platform.storage.bronze import (
    BronzeStorage,
)
from src.youtube_intelligence_platform.ingestion.channel_registry import (
    load_channels,
)
from src.youtube_intelligence_platform.ingestion.daily_discovery import (
    DailyDiscovery,
)
from src.youtube_intelligence_platform.ingestion.discovery_state import (
    DiscoveryState,
)
from src.youtube_intelligence_platform.ingestion.postgres_pending_queue import (
    PostgresPendingQueue,
)
from src.youtube_intelligence_platform.ingestion.postgres_ingested_registry import (
    PostgresIngestedRegistry,
)
from src.youtube_intelligence_platform.ingestion.postgres_completion import (
    PostgresIngestionCompletion,
)
from src.youtube_intelligence_platform.ingestion.postgres_daily_worker import (
    PostgresDailyVideoWorker,
)
from src.youtube_intelligence_platform.database.connection import (
    get_db_connection,
)


MAX_PAGES = 2
BATCH_SIZE = 50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def check_registered_channel(channel_id):
    """Verify the channel exists before inserting tasks."""

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM ingestion.channels
                WHERE channel_id = %s
                  AND enabled = TRUE
                """,
                (channel_id,),
            )

            if cursor.fetchone() is None:
                raise ValueError(
                    f"Channel {channel_id} is not registered "
                    "and enabled in PostgreSQL"
                )


def main():
    channels = load_channels()

    queue = PostgresPendingQueue()
    registry = PostgresIngestedRegistry()
    completion = PostgresIngestionCompletion()

    # Keep local Bronze for now.
    storage = BronzeStorage()

    # Discovery state is still stored locally.
    discovery_state = DiscoveryState()

    client = YouTubeClient()

    discovery = DailyDiscovery(
        client=client,
        queue=queue,
        discovery_state=discovery_state,
        registry=registry,
    )

    worker = PostgresDailyVideoWorker(
        client=client,
        storage=storage,
        queue=queue,
        registry=registry,
        completion=completion,
    )

    results = []
    failures = []

    try:
        for channel in channels:
            handle = channel["handle"]

            logger.info(
                "Starting PostgreSQL daily ingestion: %s",
                handle,
            )

            try:
                # 1. Resolve the real channel ID.
                channel_data = client.get_channel(handle)
                channel_id = channel_data["id"]

                # 2. Verify PostgreSQL channel registration.
                check_registered_channel(channel_id)

                # 3. Discover and queue missing videos.
                scan = discovery.run(
                    channel_handle=handle,
                    max_pages=MAX_PAGES,
                )

                if scan["channel_id"] != channel_id:
                    raise ValueError(
                        "Channel ID changed during discovery"
                    )

                logger.info(
                    "%s | Newly queued: %d | Pending: %d",
                    handle,
                    scan["newly_queued"],
                    scan["pending_count"],
                )

                # 4. Process pending tasks.
                saved = 0

                while True:
                    pending_before = len(
                        queue.load(channel_id)[
                            "pending_video_ids"
                        ]
                    )

                    if pending_before == 0:
                        break

                    result = worker.run(
                        channel_id,
                        batch_size=BATCH_SIZE,
                    )

                    saved += result["saved"]

                    pending_after = result["remaining"]

                    # Avoid endlessly retrying unavailable videos.
                    if pending_after >= pending_before:
                        logger.warning(
                            "%s: no queue progress; %d pending",
                            handle,
                            pending_after,
                        )
                        break

                remaining = len(
                    queue.load(channel_id)[
                        "pending_video_ids"
                    ]
                )

                results.append({
                    "handle": handle,
                    "queued": scan["newly_queued"],
                    "saved": saved,
                    "remaining": remaining,
                })

                if remaining > 0:
                    failures.append(handle)

            except Exception:
                logger.exception(
                    "Ingestion failed for %s",
                    handle,
                )
                failures.append(handle)

    finally:
        client.close()

    print(
        "\n========== POSTGRESQL DAILY INGESTION =========="
    )

    for result in results:
        print(
            f"{result['handle']} | "
            f"Queued: {result['queued']} | "
            f"Saved: {result['saved']} | "
            f"Remaining: {result['remaining']}"
        )

    if failures:
        print(
            "Incomplete channels:",
            ", ".join(sorted(set(failures))),
        )
        return 1

    print("PostgreSQL daily ingestion completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
