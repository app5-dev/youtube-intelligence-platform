
import logging
import sys

from src.youtube_intelligence_platform.api.youtube_client import (
    YouTubeClient,
)
from src.youtube_intelligence_platform.storage.bronze import (
    BronzeStorage,
)
from src.youtube_intelligence_platform.ingestion.video_backfill import (
    VideoBackfill,
)
from src.youtube_intelligence_platform.ingestion.channel_registry import (
    load_channels,
)


MAX_VIDEOS_PER_CHANNEL = 100

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    channels = load_channels()

    logger.info(
        "Starting multi-channel backfill for %d channels",
        len(channels),
    )

    client = YouTubeClient()
    storage = BronzeStorage()

    pipeline = VideoBackfill(
        client=client,
        storage=storage,
    )

    results = []
    failures = []

    try:
        for channel in channels:
            handle = channel["handle"]
            category = channel["category"]

            logger.info(
                "Starting channel %s (%s)",
                handle,
                category,
            )

            try:
                result = pipeline.run(
                    channel_handle=handle,
                    max_videos=MAX_VIDEOS_PER_CHANNEL,
                )

                results.append({
                    "handle": handle,
                    "category": category,
                    **result,
                })

                logger.info(
                    "Completed %s: %d videos saved",
                    handle,
                    result["saved"],
                )

            except Exception:
                logger.exception(
                    "Backfill failed for %s",
                    handle,
                )
                failures.append(handle)

    finally:
        client.close()

    print("\n========== BACKFILL SUMMARY ==========")

    for result in results:
        print(
            f"{result['handle']} | "
            f"Saved: {result['saved']} | "
            f"Total: {result['total_checkpointed']}"
        )

    if failures:
        print("\nFailed channels:", ", ".join(failures))
        sys.exit(1)

    print("\nAll enabled channels processed successfully.")


if __name__ == "__main__":
    main()
