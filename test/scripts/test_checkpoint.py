

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from src.youtube_intelligence_platform.checkpoint import (
    load_checkpoint,
    save_checkpoint,
    mark_videos_ingested,
)


with TemporaryDirectory() as temp_dir:
    checkpoint_path = (
        Path(temp_dir) / "google_developers.json"
    )
    channel_id = "UC_TEST_123"

    # First run: no checkpoint exists.
    checkpoint = load_checkpoint(
        checkpoint_path, channel_id
    )

    assert checkpoint["status"] == "NOT_STARTED"
    print("Initial status:", checkpoint["status"])

    # Simulate a successful first batch of 50 videos.
    batch_1 = [f"video_{i}" for i in range(1, 51)]

    checkpoint = mark_videos_ingested(
        checkpoint, batch_1
    )
    save_checkpoint(checkpoint_path, checkpoint)

    # Simulate a new execution of the pipeline.
    checkpoint = load_checkpoint(
        checkpoint_path, channel_id
    )

    print("Recovered videos:", len(
        checkpoint["processed_video_ids"]
    ))

    assert len(checkpoint["processed_video_ids"]) == 50
    assert checkpoint["last_processed_video_id"] == "video_50"

    # Simulate a second successful batch.
    batch_2 = [f"video_{i}" for i in range(51, 101)]

    checkpoint = mark_videos_ingested(
        checkpoint, batch_2
    )
    save_checkpoint(checkpoint_path, checkpoint)

    recovered = load_checkpoint(
        checkpoint_path, channel_id
    )

    assert len(recovered["processed_video_ids"]) == 100
    assert recovered["last_processed_video_id"] == "video_100"

    print("Total ingested:", len(
        recovered["processed_video_ids"]
    ))
    print("Last processed:", recovered["last_processed_video_id"])
    print("Checkpoint recovery test: PASSED")