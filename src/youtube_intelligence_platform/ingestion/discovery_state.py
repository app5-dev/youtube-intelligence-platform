
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


class DiscoveryState:
    def __init__(self, base_path="data/checkpoints/discovery"):
        self.base_path = Path(base_path)

    def save(self, result):
        channel_id = result["channel_id"]

        self.base_path.mkdir(
            parents=True,
            exist_ok=True,
        )

        destination = (
            self.base_path / f"{channel_id}_discovery.json"
        )

        document = {
            "channel_id": channel_id,
            "channel_handle": result["channel_handle"],
            "last_scan_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "status": "SUCCESS",
            "pages_scanned": result["pages_scanned"],
            "videos_discovered": result["videos_discovered"],
            "known_videos": result["known_videos"],
            "missing_video_count": len(
                result["new_video_ids"]
            ),
            "coverage": "RECENT_WINDOW_ONLY",
        }

        temp_path = None

        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.base_path,
                suffix=".tmp",
                delete=False,
            ) as file:
                temp_path = Path(file.name)

                json.dump(document, file, indent=2)
                file.flush()
                os.fsync(file.fileno())

            os.replace(temp_path, destination)

        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)

        return destination
