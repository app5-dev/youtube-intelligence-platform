
from src.youtube_intelligence_platform.ingestion.video_discovery import (
    VideoDiscovery,
)


class DailyDiscovery:
    def __init__(
        self,
        client,
        queue,
        discovery_state,
        registry=None,
        checkpoint_dir="data/checkpoints",
    ):
        self.discovery = VideoDiscovery(
            client=client,
            checkpoint_dir=checkpoint_dir,
            registry=registry,
        )

        self.queue = queue
        self.discovery_state = discovery_state

    def run(self, channel_handle, max_pages=2):
        # 1. Discover video IDs.
        result = self.discovery.discover(
            channel_handle=channel_handle,
            max_pages=max_pages,
        )

        channel_id = result["channel_id"]

        # 2. Persist missing IDs before marking scan successful.
        added = self.queue.enqueue(
            channel_id,
            result["new_video_ids"],
        )

        # 3. Save discovery metadata.
        state_path = self.discovery_state.save(result)

        # 4. Count pending tasks.
        pending_count = len(
            self.queue.load(channel_id)["pending_video_ids"]
        )

        return {
            **result,
            "newly_queued": added,
            "pending_count": pending_count,
            "state_path": str(state_path),
        }

