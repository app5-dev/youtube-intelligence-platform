
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from src.youtube_intelligence_platform.checkpoint import (
    load_checkpoint,
)


class IngestedVideoRegistry:
    def __init__(
        self,
        checkpoint_dir="data/checkpoints",
    ):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.daily_dir = self.checkpoint_dir / "daily_ingested"

    def _daily_path(self, channel_id):
        if not channel_id or not all(
            c.isalnum() or c in "_-" for c in channel_id
        ):
            raise ValueError("Invalid channel ID")

        return self.daily_dir / f"{channel_id}.json"

    def get_ingested_ids(self, channel_id):
        """Read completed IDs from both ingestion workflows."""

        backfill_path = (
            self.checkpoint_dir
            / f"{channel_id}_backfill.json"
        )

        backfill = load_checkpoint(
            backfill_path, channel_id
        )

        known_ids = set(
            backfill["processed_video_ids"]
        )

        daily_path = self._daily_path(channel_id)

        if daily_path.exists():
            with daily_path.open(
                "r", encoding="utf-8"
            ) as file:
                daily = json.load(file)

            if daily["channel_id"] != channel_id:
                raise ValueError("Wrong channel in daily registry")

            known_ids.update(daily["video_ids"])

        return known_ids

    def mark_daily_ingested(self, channel_id, video_ids):
        """Record IDs only after Bronze persistence succeeds."""

        path = self._daily_path(channel_id)
        path.parent.mkdir(parents=True, exist_ok=True)

        existing = set()

        if path.exists():
            with path.open("r", encoding="utf-8") as file:
                document = json.load(file)

            if document["channel_id"] != channel_id:
                raise ValueError("Wrong channel in daily registry")

            existing.update(document["video_ids"])

        existing.update(video_ids)

        document = {
            "channel_id": channel_id,
            "video_ids": sorted(existing),
            "updated_at": datetime.now(
                timezone.utc
            ).isoformat(),
        }

        temp_path = None

        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=path.parent,
                suffix=".tmp",
                delete=False,
            ) as file:
                temp_path = Path(file.name)
                json.dump(document, file, indent=2)
                file.flush()
                os.fsync(file.fileno())

            os.replace(temp_path, path)

        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)

        return path
