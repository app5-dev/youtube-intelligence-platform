
import json
from tempfile import TemporaryDirectory

from src.youtube_intelligence_platform.storage.bronze import (
    BronzeStorage,
)


with TemporaryDirectory() as temp_dir:
    storage = BronzeStorage(base_path=temp_dir)

    sample_videos = [
        {
            "id": "video_1",
            "snippet": {"title": "Test Video"},
            "statistics": {"viewCount": "1000"},
        }
    ]

    path = storage.save_videos(
        sample_videos,
        channel_id="UC_TEST_123",
    )

    assert path.exists()

    with path.open("r", encoding="utf-8") as file:
        document = json.load(file)

    assert document["metadata"]["record_count"] == 1
    assert document["metadata"]["channel_id"] == "UC_TEST_123"
    assert document["raw"][0]["id"] == "video_1"

    print("Bronze storage test: PASSED")
    print("Saved records:", document["metadata"]["record_count"])
