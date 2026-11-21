
from src.youtube_intelligence_platform.api.youtube_client import (
    YouTubeClient,
)

from src.youtube_intelligence_platform.ingestion.channel_registry import (
    load_channels,
)

from src.youtube_intelligence_platform.ingestion.video_discovery import (
    VideoDiscovery,
)


def main():
    channels = load_channels()
    client = YouTubeClient()

    try:
        discovery = VideoDiscovery(client)

        for channel in channels:
            handle = channel["handle"]

            result = discovery.discover(
                channel_handle=handle,
                max_pages=2,
            )

            print(f"\nChannel: {handle}")
            print("Pages scanned:", result["pages_scanned"])
            print(
                "Videos discovered:",
                result["videos_discovered"],
            )
            print(
                "Already known:",
                result["known_videos"],
            )
            print(
                "New video IDs:",
                result["new_video_ids"],
            )

    finally:
        client.close()


if __name__ == "__main__":
    main()
