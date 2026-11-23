
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


class PendingVideoQueue:
    def __init__(self, base_path="data/checkpoints/pending"):
        self.base_path = Path(base_path)

    def _path(self, channel_id):
        if not channel_id or not channel_id.replace("_", "").isalnum():
            raise ValueError("Invalid channel_id")

        return self.base_path / f"{channel_id}_pending.json"

    def load(self, channel_id):
        path = self._path(channel_id)

        if not path.exists():
            return {
                "channel_id": channel_id,
                "pending_video_ids": [],
                "updated_at": None,
            }

        with path.open("r", encoding="utf-8") as file:
            document = json.load(file)

        if document["channel_id"] != channel_id:
            raise ValueError("Queue belongs to another channel")

        ids = document["pending_video_ids"]

        if not isinstance(ids, list) or len(ids) != len(set(ids)):
            raise ValueError("Invalid pending video IDs")

        return document

    def _save(self, channel_id, document):
        path = self._path(channel_id)
        path.parent.mkdir(parents=True, exist_ok=True)

        document["updated_at"] = (
            datetime.now(timezone.utc).isoformat()
        )

        temporary = None

        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=path.parent,
                suffix=".tmp",
                delete=False,
            ) as file:
                temporary = Path(file.name)

                json.dump(document, file, indent=2)

                file.flush()
                os.fsync(file.fileno())

            os.replace(temporary, path)

        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

        return path

    def enqueue(self, channel_id, video_ids):
        """Add missing IDs without duplicating existing entries."""
        document = self.load(channel_id)

        pending = document["pending_video_ids"]
        seen = set(pending)

        added = 0

        for video_id in video_ids:
            if not isinstance(video_id, str) or not video_id:
                raise ValueError("Invalid video ID")

            if video_id not in seen:
                pending.append(video_id)
                seen.add(video_id)
                added += 1

        if added:
            self._save(channel_id, document)

        return added

    def peek(self, channel_id, limit=50):
        """Read pending IDs without removing them."""
        if limit < 1:
            raise ValueError("limit must be positive")

        document = self.load(channel_id)

        return document["pending_video_ids"][:limit]

    def acknowledge(self, channel_id, completed_ids):
        """Remove IDs only after successful processing."""
        document = self.load(channel_id)

        completed = set(completed_ids)

        before = len(document["pending_video_ids"])

        document["pending_video_ids"] = [
            video_id
            for video_id in document["pending_video_ids"]
            if video_id not in completed
        ]

        removed = before - len(document["pending_video_ids"])

        if removed:
            self._save(channel_id, document)

        return removed
