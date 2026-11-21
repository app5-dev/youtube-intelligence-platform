#Coordinate historical video ingestion and checkpoints

from pathlib import Path

from src.youtube_intelligence_platform.checkpoint import (
    load_checkpoint,
    save_checkpoint,
    mark_videos_ingested,
)


class VideoBackfill:
    def __init__(
        self,
        client,
        storage,
        checkpoint_dir="data/checkpoints",
    ):
        self.client = client
        self.storage = storage
        self.checkpoint_dir = Path(checkpoint_dir)

    def discover_video_ids(
        self, playlist_id, processed_ids, limit
    ):
        """Discover unprocessed videos using pagination."""
        new_ids = []
        seen = set()
        page_token = None

        while len(new_ids) < limit:
            page = self.client.get_playlist_page(
                playlist_id, page_token
            )

            for item in page.get("items", []):
                video_id = item["contentDetails"]["videoId"]

                if video_id in seen:
                    continue

                seen.add(video_id)

                if video_id not in processed_ids:
                    new_ids.append(video_id)

                if len(new_ids) >= limit:
                    break

            page_token = page.get("nextPageToken")

            if not page_token:
                break

        return new_ids

    def run(self, channel_handle, max_videos=100):
        if max_videos < 1:
            raise ValueError("max_videos must be positive")

        # 1. Resolve the YouTube channel.
        channel = self.client.get_channel(channel_handle)
        channel_id = channel["id"]
        playlist_id = channel[
            "contentDetails"
        ]["relatedPlaylists"]["uploads"]

        # 2. Load existing checkpoint.
        checkpoint_path = (
            self.checkpoint_dir
            / f"{channel_id}_backfill.json"
        )

        checkpoint = load_checkpoint(
            checkpoint_path, channel_id
        )

        processed_ids = set(
            checkpoint["processed_video_ids"]
        )

        print("Previously ingested:", len(processed_ids))

        # 3. Discover unprocessed video IDs.
        new_ids = self.discover_video_ids(
            playlist_id,
            processed_ids,
            max_videos,
        )

        print("Videos selected:", len(new_ids))

        saved_count = 0

        # 4. Extract metadata in batches of 50.
        for start in range(0, len(new_ids), 50):
            batch_ids = new_ids[start:start + 50]

            videos = self.client.get_video_details(
                batch_ids
            )

            if not videos:
                print("Empty batch: skipped")
                continue

            # 5. Save data to Bronze FIRST.
            path = self.storage.save_videos(
                videos, channel_id
            )

            # 6. Update checkpoint AFTER saving.
            completed_ids = [
                video["id"] for video in videos
            ]

            mark_videos_ingested(
                checkpoint, completed_ids
            )

            save_checkpoint(
                checkpoint_path, checkpoint
            )

            saved_count += len(completed_ids)

            print(
                f"Saved {len(completed_ids)} videos -> {path}"
            )

        print(
            "Total checkpointed:",
            len(checkpoint["processed_video_ids"]),
        )

        return {
            "channel_id": channel_id,
            "selected": len(new_ids),
            "saved": saved_count,
            "total_checkpointed": len(
                checkpoint["processed_video_ids"]
            ),
        }
