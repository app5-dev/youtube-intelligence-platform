
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import src.youtube_intelligence_platform.ingestion.video_backfill as backfill_module

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
                {"contentDetails": {"videoId": f"V{i}"}}
                for i in range(1, 4)
            ]
        }

    def get_video_details(self, video_ids):
        return [
            {"id": video_id}
            for video_id in video_ids
        ]


with TemporaryDirectory() as temp_dir:
    root = Path(temp_dir)

    storage = BronzeStorage(root / "bronze")
    checkpoint_dir = root / "checkpoints"

    pipeline = VideoBackfill(
        client=FakeYouTubeClient(),
        storage=storage,
        checkpoint_dir=checkpoint_dir,
    )

    checkpoint_path = checkpoint_dir / "UC_TEST_backfill.json"

    # First execution: simulate checkpoint write failure
    # AFTER the Bronze batch has been persisted.
    with patch.object(
        backfill_module,
        "save_checkpoint",
        side_effect=OSError("Simulated checkpoint failure"),
    ):
        try:
            pipeline.run("@TestChannel", max_videos=3)
            raise AssertionError("Expected checkpoint failure")
        except OSError as exc:
            print("Expected failure:", exc)

    bronze_files = list(
        (root / "bronze").rglob("batch_*.json")
    )

    assert len(bronze_files) == 1

    checkpoint = load_checkpoint(
        checkpoint_path, "UC_TEST"
    )

    assert len(checkpoint["processed_video_ids"]) == 0

    print("Bronze files after crash:", len(bronze_files))
    print(
        "Checkpointed IDs after crash:",
        len(checkpoint["processed_video_ids"]),
    )

    # Second execution: retry with working checkpoint storage.
    result = pipeline.run("@TestChannel", max_videos=3)

    bronze_files = list(
        (root / "bronze").rglob("batch_*.json")
    )

    checkpoint = load_checkpoint(
        checkpoint_path, "UC_TEST"
    )

    assert len(bronze_files) == 1
    assert result["total_checkpointed"] == 3
    assert len(checkpoint["processed_video_ids"]) == 3

    print("Bronze files after recovery:", len(bronze_files))
    print("Checkpointed IDs:", len(checkpoint["processed_video_ids"]))
    print("Crash consistency test: PASSED")
