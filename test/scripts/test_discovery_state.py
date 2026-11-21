
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from src.youtube_intelligence_platform.ingestion.discovery_state import (
    DiscoveryState,
)


with TemporaryDirectory() as temp_dir:
    state = DiscoveryState(base_path=temp_dir)

    result = {
        "channel_id": "UC_TEST",
        "channel_handle": "@TestChannel",
        "pages_scanned": 2,
        "videos_discovered": 100,
        "known_videos": 97,
        "new_video_ids": ["V101", "V102", "V103"],
    }

    path = state.save(result)

    assert path.exists()

    with path.open(encoding="utf-8") as file:
        document = json.load(file)

    assert document["status"] == "SUCCESS"
    assert document["pages_scanned"] == 2
    assert document["missing_video_count"] == 3
    assert document["coverage"] == "RECENT_WINDOW_ONLY"

    print("Channel:", document["channel_id"])
    print("Scan status:", document["status"])
    print("Pages scanned:", document["pages_scanned"])
    print("Missing videos:", document["missing_video_count"])
    print("Discovery state test: PASSED")
