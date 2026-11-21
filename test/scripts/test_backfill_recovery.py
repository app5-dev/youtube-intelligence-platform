
from pathlib import Path
from tempfile import TemporaryDirectory

from src.youtube_intelligence_platform.checkpoint import (
    load_checkpoint,
)
from src.youtube_intelligence_platform.ingestion.video_backfill import (
    VideoBackfill,
)
from src.youtube_intelligence_platform.storage.bronze import (
    BronzeStorage,
)


class FakeYouTubeClient:
    def get_channel(self, handle):
        return {
            "id": "UC_TEST",
            "contentDetails": {
                "relatedPlaylists": {
                    "uploads": "UPLOADS_TEST"
                }
            },
        }

    def get_playlist_page(self, playlist_id, page_token=None):
        return {
            "items": [
                {"contentDetails": {"videoId": f"video_{i}"}}
                for i in range(1, 61)
            ]
        }

    def get_video_details(self, video_ids):
        return [
            {"id": video_id}
            for video_id in video_ids
        ]


class FailingStorage:
    def __init__(self, real_storage):
        self.real_storage = real_storage
        self.calls = 0

    def save_videos(self, videos, channel_id):
        self.calls += 1

        if self.calls == 2:
            raise OSError("Simulated storage failure")

        return self.real_storage.save_videos(
            videos, channel_id
        )


with TemporaryDirectory() as temp_dir:
    root = Path(temp_dir)

    client = FakeYouTubeClient()

    real_storage = BronzeStorage(
        base_path=root / "bronze"
    )

    failing_storage = FailingStorage(real_storage)

    checkpoint_dir = root / "checkpoints"
    checkpoint_path = (
        checkpoint_dir / "UC_TEST_backfill.json"
    )

    pipeline = VideoBackfill(
        client=client,
        storage=failing_storage,
        checkpoint_dir=checkpoint_dir,
    )

    # FIRST EXECUTION — second batch fails
    try:
        pipeline.run("@TestChannel", max_videos=60)
        raise AssertionError("Expected storage failure")

    except OSError as exc:
        print("Expected failure:", exc)

    # Check that only the first 50 are checkpointed
    checkpoint = load_checkpoint(
        checkpoint_path, "UC_TEST"
    )

    assert len(checkpoint["processed_video_ids"]) == 50

    print(
        "Checkpoint after failure:",
        len(checkpoint["processed_video_ids"])
    )

    # SECOND EXECUTION — use functioning storage
    recovered_pipeline = VideoBackfill(
        client=client,
        storage=real_storage,
        checkpoint_dir=checkpoint_dir,
    )

    result = recovered_pipeline.run(
        "@TestChannel", max_videos=60
    )

    assert result["saved"] == 10
    assert result["total_checkpointed"] == 60

    # Verify no duplicate persisted batch
    files = list((root / "bronze").rglob("batch_*.json"))

    assert len(files) == 2

    print("Total ingested:", result["total_checkpointed"])
    print("Bronze files:", len(files))
    print("Failure recovery test: PASSED")
