from src.youtube_intelligence_platform.storage.base import (
    VideoBronzeStorage,
)

class DailyVideoWorker:
    def __init__(self, client, storage: VideoBronzeStorage, queue, registry):
        self.client = client
        self.storage = storage
        self.queue = queue
        self.registry = registry

    def run(self, channel_id, batch_size=50):
        if not 1 <= batch_size <= 50:
            raise ValueError(
                "batch_size must be between 1 and 50"
            )

        # 1. Read pending IDs without removing them.
        pending_ids = self.queue.peek(
            channel_id, limit=batch_size
        )

        if not pending_ids:
            print("No pending videos")
            return {"saved": 0, "remaining": 0}

        print("Pending videos:", len(pending_ids))

        # 2. Check which IDs are already ingested.
        known_ids = self.registry.get_ingested_ids(
            channel_id
        )

        already_ingested = [
            video_id
            for video_id in pending_ids
            if video_id in known_ids
        ]

        # Remove stale work already marked as ingested.
        if already_ingested:
            self.queue.acknowledge(
                channel_id, already_ingested
            )

        new_ids = [
            video_id
            for video_id in pending_ids
            if video_id not in known_ids
        ]

        if not new_ids:
            print("All pending videos already ingested")
            return {
                "saved": 0,
                "remaining": len(
                    self.queue.load(channel_id)["pending_video_ids"]
                ),
            }

        # 3. Retrieve details from YouTube.
        videos = self.client.get_video_details(new_ids)

        requested = set(new_ids)

        returned_ids = [video["id"] for video in videos]

        if (
            len(returned_ids) != len(set(returned_ids))
            or not set(returned_ids).issubset(requested)
        ):
            raise ValueError(
                "Unexpected or duplicate video IDs in API response"
            )

        if not videos:
            print("No video details returned; IDs remain pending")
            return {
                "saved": 0,
                "remaining": len(
                    self.queue.load(channel_id)["pending_video_ids"]
                ),
            }

        # 4. Save to Bronze.
        path = self.storage.save_videos(
            videos,
            channel_id,
            operation="video_initial",
        )

        print("Bronze saved:", path)

        # 5. Record successfully persisted IDs.
        self.registry.mark_daily_ingested(
            channel_id, returned_ids
        )

        # 6. Acknowledge only completed IDs.
        self.queue.acknowledge(
            channel_id, returned_ids
        )

        remaining = len(
            self.queue.load(channel_id)["pending_video_ids"]
        )

        print("Videos saved:", len(returned_ids))
        print("Remaining pending:", remaining)

        return {
            "saved": len(returned_ids),
            "remaining": remaining,
        }
