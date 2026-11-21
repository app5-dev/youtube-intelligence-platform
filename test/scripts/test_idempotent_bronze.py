
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from src.youtube_intelligence_platform.storage.bronze import (
    BronzeStorage,
)


with TemporaryDirectory() as temp_dir:
    storage = BronzeStorage(base_path=temp_dir)

    videos = [
        {"id": "V100", "statistics": {"viewCount": "1000"}},
        {"id": "V101", "statistics": {"viewCount": "2000"}},
    ]

    # First write
    path_1 = storage.save_videos(videos, "UC_TEST")
    print("First write:", path_1.name)

    # Retry with the same videos in reverse order
    path_2 = storage.save_videos(
        list(reversed(videos)), "UC_TEST"
    )

    print("Second write:", path_2.name)

    assert path_1 == path_2

    # Only one physical batch should exist
    files = list(Path(temp_dir).rglob("batch_*.json"))
    assert len(files) == 1

    with path_1.open(encoding="utf-8") as file:
        document = json.load(file)

    assert len(document["raw"]) == 2
    assert document["metadata"]["record_count"] == 2

    print("Physical Bronze files:", len(files))
    print("Idempotent Bronze test: PASSED")
