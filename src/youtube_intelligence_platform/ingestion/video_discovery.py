
from pathlib import Path

from src.youtube_intelligence_platform.checkpoint import (
    load_checkpoint,
)


class VideoDiscovery:
    def __init__(
        self,
        client,
        checkpoint_dir="data/checkpoints",
    ):
        self.client = client
        self.checkpoint_dir = Path(checkpoint_dir)

    def discover(self, channel_handle, max_pages=2):
        if max_pages < 1:
            raise ValueError("max_pages must be positive")

        channel = self.client.get_channel(channel_handle)
        channel_id = channel["id"]

        playlist_id = channel[
            "contentDetails"
        ]["relatedPlaylists"]["uploads"]

        checkpoint_path = (
            self.checkpoint_dir
            / f"{channel_id}_backfill.json"
        )

        checkpoint = load_checkpoint(
            checkpoint_path, channel_id
        )

        known_ids = set(
            checkpoint["processed_video_ids"]
        )

        discovered_ids = []
        seen = set()
        page_token = None
        pages_scanned = 0

        for _ in range(max_pages):
            response = self.client.get_playlist_page(
                playlist_id, page_token
            )
            pages_scanned += 1

            for item in response.get("items", []):
                video_id = item["contentDetails"]["videoId"]

                if video_id not in seen:
                    seen.add(video_id)
                    discovered_ids.append(video_id)

            page_token = response.get("nextPageToken")

            if not page_token:
                break

        new_ids = [
            video_id
            for video_id in discovered_ids
            if video_id not in known_ids
        ]

        return {
            "channel_id": channel_id,
            "channel_handle": channel_handle,
            "pages_scanned": pages_scanned,
            "videos_discovered": len(discovered_ids),
            "known_videos": len(discovered_ids) - len(new_ids),
            "new_video_ids": new_ids,
        }
