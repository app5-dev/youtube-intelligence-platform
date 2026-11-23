
from pathlib import Path
from tempfile import TemporaryDirectory

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
    def get_video_details(self, video_ids):
        return [
            {
                "id": video_id,
                "snippet": {"title": f"Video {video_id}"},
                "statistics": {"viewCount": "1000"},
            }
            for video_id in video_ids
        ]


with TemporaryDirectory() as temp_dir:
    root = Path(temp_dir)
    channel_id = "UC_TEST"

    queue = PendingVideoQueue(root / "checkpoints/pending")
    registry = IngestedVideoRegistry(root / "checkpoints")
    storage = BronzeStorage(root / "bronze")

    worker = DailyVideoWorker(
        client=FakeYouTubeClient(),
        storage=storage,
        queue=queue,
        registry=registry,
    )

    # Simulate daily discovery.
    queue.enqueue(
        channel_id, ["V501", "V502", "V503"]
    )

    # First execution.
    result_1 = worker.run(channel_id)

    assert result_1["saved"] == 3
    assert result_1["remaining"] == 0

    # Verify the shared registry.
    known_ids = registry.get_ingested_ids(channel_id)

    assert known_ids == {"V501", "V502", "V503"}

    # Verify Bronze persistence.
    bronze_files = list(
        (root / "bronze").rglob("batch_*.json")
    )

    assert len(bronze_files) == 1

    # Verify the correct ingestion operation.
    assert "initial" in bronze_files[0].parts

    # Simulate repeated discovery.
    queue.enqueue(channel_id, ["V501", "V502", "V503"])

    # Already-ingested IDs must not be fetched again.
    result_2 = worker.run(channel_id)

    assert result_2["saved"] == 0
    assert result_2["remaining"] == 0

    bronze_files = list(
        (root / "bronze").rglob("batch_*.json")
    )

    assert len(bronze_files) == 1

    assert "initial" in bronze_files[0].parts

    print("First run saved:", result_1["saved"])
    print("Second run saved:", result_2["saved"])
    print("Bronze files:", len(bronze_files))
    print("Daily video worker test: PASSED")
