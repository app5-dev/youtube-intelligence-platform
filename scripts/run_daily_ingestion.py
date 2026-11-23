
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
from src.youtube_intelligence_platform.ingestion.pending_queue import (
    PendingVideoQueue,
)
from src.youtube_intelligence_platform.ingestion.discovery_state import (
    DiscoveryState,
)
from src.youtube_intelligence_platform.ingestion.ingested_registry import (
    IngestedVideoRegistry,
)
from src.youtube_intelligence_platform.ingestion.daily_video_worker import (
    DailyVideoWorker,
)


MAX_PAGES = 2
BATCH_SIZE = 50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    channels = load_channels()

    client = YouTubeClient()
    storage = BronzeStorage()

    queue = PendingVideoQueue()
    registry = IngestedVideoRegistry()
    discovery_state = DiscoveryState()

    discovery = DailyDiscovery(
        client=client,
        queue=queue,
        discovery_state=discovery_state,
        registry=registry,
    )

    worker = DailyVideoWorker(
        client=client,
        storage=storage,
        queue=queue,
        registry=registry,
    )

    results = []
    failures = []

    try:
        for channel in channels:
            handle = channel["handle"]

            logger.info("Starting daily ingestion: %s", handle)

            try:
                # 1. Discover candidates and persist pending IDs.
                scan = discovery.run(
                    channel_handle=handle,
                    max_pages=MAX_PAGES,
                )

                channel_id = scan["channel_id"]

                logger.info(
                    "%s: %d newly queued, %d total pending",
                    handle,
                    scan["newly_queued"],
                    scan["pending_count"],
                )

                # 2. Drain pending work in batches of up to 50.
                saved = 0

                while True:
                    pending_before = len(
                        queue.load(channel_id)["pending_video_ids"]
                    )

                    if pending_before == 0:
                        break

                    result = worker.run(
                        channel_id,
                        batch_size=BATCH_SIZE,
                    )

                    saved += result["saved"]

                    pending_after = result["remaining"]

                    # Stop only if the queue made no progress.
                    if pending_after >= pending_before:
                        logger.warning(
                            "%s: no queue progress; %d remain",
                            handle,
                            pending_after,
                        )
                        break

                remaining = len(
                    queue.load(channel_id)["pending_video_ids"]
                )

                results.append({
                    "handle": handle,
                    "queued": scan["newly_queued"],
                    "saved": saved,
                    "remaining": remaining,
                })

                if remaining:
                    failures.append(handle)

            except Exception:
                logger.exception(
                    "Daily ingestion failed for %s", handle
                )
                failures.append(handle)

    finally:
        client.close()

    print("\n========== DAILY INGESTION SUMMARY ==========")

    for result in results:
        print(
            f"{result['handle']} | "
            f"Queued: {result['queued']} | "
            f"Saved: {result['saved']} | "
            f"Remaining: {result['remaining']}"
        )

    if failures:
        print("Incomplete channels:", ", ".join(set(failures)))
        sys.exit(1)

    print("Daily ingestion completed successfully.")


if __name__ == "__main__":
    main()
