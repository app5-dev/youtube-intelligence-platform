
from pathlib import Path
from tempfile import TemporaryDirectory

from src.youtube_intelligence_platform.storage.bronze import (
    BronzeStorage,
)


videos = [
    {"id": "V501", "snippet": {"title": "Video 501"}},
    {"id": "V502", "snippet": {"title": "Video 502"}},
]

with TemporaryDirectory() as temp_dir:
    storage = BronzeStorage(Path(temp_dir))
    channel_id = "UC_TEST"

    # Existing operation: backward compatibility
    legacy_path = Path(storage.save_videos(
        videos,
        channel_id,
        operation="historical_backfill",
    ))

    assert legacy_path.exists()
    assert "backfill" in legacy_path.parts

    # New operation
    initial_path = Path(storage.save_videos(
        videos,
        channel_id,
        operation="video_initial",
    ))

    assert initial_path.exists()
    assert "initial" in initial_path.parts

    # Different operations must have distinct files.
    assert legacy_path != initial_path

    # Retrying the initial operation must reuse its file.
    retry_path = Path(storage.save_videos(
        list(reversed(videos)),
        channel_id,
        operation="video_initial",
    ))

    assert retry_path == initial_path

    files = list(Path(temp_dir).rglob("batch_*.json"))
    assert len(files) == 2

    print("Legacy operation: PASSED")
    print("Initial operation: PASSED")
    print("Retry idempotency: PASSED")
    print("Bronze operation tests: PASSED")
