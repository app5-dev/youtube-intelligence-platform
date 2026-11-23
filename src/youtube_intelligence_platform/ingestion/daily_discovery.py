
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
    ):
        self.discovery = VideoDiscovery(
            client=client,
            checkpoint_dir=(
                registry.checkpoint_dir
                if registry is not None
                else "data/checkpoints"
            ),
            registry=registry,
        )

        self.queue = queue
        self.discovery_state = discovery_state

    def run(self, channel_handle, max_pages=2):
        # Step 1: Discover video IDs
        result = self.discovery.discover(
            channel_handle=channel_handle,
            max_pages=max_pages,
        )

        channel_id = result["channel_id"]

        # Step 2: Persist pending video IDs
        added = self.queue.enqueue(
            channel_id,
            result["new_video_ids"],
        )

        # Step 3: Save discovery state only after queuing
        state_path = self.discovery_state.save(result)

        pending_count = len(
            self.queue.load(channel_id)["pending_video_ids"]
        )

        return {
            **result,
            "newly_queued": added,
            "pending_count": pending_count,
            "state_path": str(state_path),
        }

