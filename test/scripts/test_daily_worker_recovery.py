
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from src.youtube_intelligence_platform.ingestion.daily_video_worker import (
    DailyVideoWorker,
)
from src.youtube_intelligence_platform.ingestion.pending_queue import (
    PendingVideoQueue,
)
from src.youtube_intelligence_platform.ingestion.ingested_registry import (
    IngestedVideoRegistry,
)
from src.youtube_intelligence_platform.storage.bronze import (
    BronzeStorage,
)


class FakeYouTubeClient:
    def __init__(self):
        self.calls = 0

    def get_video_details(self, video_ids):
        self.calls += 1

        return [
            {
                "id": video_id,
                "snippet": {"title": f"Video {video_id}"},
                "statistics": {"viewCount": "1000"},
            }
            for video_id in video_ids
        ]


def test_recovery(failure_point):
    with TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        channel_id = "UC_TEST"
        ids = ["V501", "V502", "V503"]

        client = FakeYouTubeClient()
        queue = PendingVideoQueue(root / "checkpoints/pending")
        registry = IngestedVideoRegistry(root / "checkpoints")
        storage = BronzeStorage(root / "bronze")

        worker = DailyVideoWorker(
            client=client,
            storage=storage,
            queue=queue,
            registry=registry,
        )

        queue.enqueue(channel_id, ids)

        # Select which operation should fail.
        if failure_point == "bronze":
            target = storage
            method = "save_videos"

        elif failure_point == "registry":
            target = registry
            method = "mark_daily_ingested"

        elif failure_point == "ack":
            target = queue
            method = "acknowledge"

        else:
            raise ValueError("Unknown failure point")

        # First execution: inject one failure.
        with patch.object(
            target,
            method,
            side_effect=OSError("Simulated failure"),
        ):
            try:
                worker.run(channel_id)
                raise AssertionError("Expected failure")
            except OSError:
                print(f"Expected {failure_point} failure")

        # Work must remain in the queue.
        assert queue.peek(channel_id) == ids

        files_before = list(
            (root / "bronze").rglob("batch_*.json")
        )

        if failure_point == "bronze":
            assert len(files_before) == 0
            assert registry.get_ingested_ids(channel_id) == set()

        elif failure_point == "registry":
            assert len(files_before) == 1
            assert registry.get_ingested_ids(channel_id) == set()

        elif failure_point == "ack":
            assert len(files_before) == 1
            assert registry.get_ingested_ids(channel_id) == set(ids)

        # Second execution: retry without the injected failure.
        result = worker.run(channel_id)

        assert queue.peek(channel_id) == []
        assert registry.get_ingested_ids(channel_id) == set(ids)

        files_after = list(
            (root / "bronze").rglob("batch_*.json")
        )
        assert len(files_after) == 1

        if failure_point == "ack":
            # Registry already knows the videos:
            # no second metadata request required.
            assert result["saved"] == 0
            assert client.calls == 1
        else:
            assert result["saved"] == 3

        print(f"{failure_point.upper()} recovery: PASSED")


for point in ("bronze", "registry", "ack"):
    test_recovery(point)

print("\nAll daily worker recovery tests: PASSED")
