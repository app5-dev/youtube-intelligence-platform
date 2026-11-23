
from pathlib import Path
from tempfile import TemporaryDirectory

from src.youtube_intelligence_platform.ingestion.daily_discovery import (
    DailyDiscovery,
)
from src.youtube_intelligence_platform.ingestion.pending_queue import (
    PendingVideoQueue,
)
from src.youtube_intelligence_platform.ingestion.discovery_state import (
    DiscoveryState,
)


class FakeYouTubeClient:
    def get_channel(self, handle):
        return {
            "id": "UC_TEST",
            "contentDetails": {
                "relatedPlaylists": {"uploads": "UPLOADS_TEST"}
            },
        }

    def get_playlist_page(self, playlist_id, page_token=None):
        return {
            "items": [
                {"contentDetails": {"videoId": "V501"}},
                {"contentDetails": {"videoId": "V502"}},
                {"contentDetails": {"videoId": "V503"}},
            ]
        }


with TemporaryDirectory() as temp_dir:
    root = Path(temp_dir)

    queue = PendingVideoQueue(root / "pending")
    state = DiscoveryState(root / "discovery")

    pipeline = DailyDiscovery(
        client=FakeYouTubeClient(),
        queue=queue,
        discovery_state=state,
    )

    # First run
    result_1 = pipeline.run("@TestChannel")

    assert result_1["newly_queued"] == 3
    assert result_1["pending_count"] == 3

    # Second run: same discovery must not duplicate IDs.
    result_2 = pipeline.run("@TestChannel")

    assert result_2["newly_queued"] == 0
    assert result_2["pending_count"] == 3

    print("First run queued:", result_1["newly_queued"])
    print("Second run queued:", result_2["newly_queued"])
    print("Pending IDs:", queue.peek("UC_TEST"))
    print("Daily discovery test: PASSED")
