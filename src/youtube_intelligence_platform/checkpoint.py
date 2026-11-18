
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def load_checkpoint(path, channel_id):
    path = Path(path)

    if not path.exists():
        return {
            "channel_id": channel_id,
            "job": "historical_backfill",
            "status": "NOT_STARTED",
            "last_processed_video_id": None,
            "processed_video_ids": [],
            "updated_at": None,
        }

    with path.open("r", encoding="utf-8") as file:
        checkpoint = json.load(file)

    if checkpoint["channel_id"] != channel_id:
        raise ValueError("Checkpoint belongs to another channel")

    return checkpoint


def save_checkpoint(path, checkpoint):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    checkpoint["updated_at"] = (
        datetime.now(timezone.utc).isoformat()
    )

    # Atomic file replacement to avoid partially written JSON.
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
            json.dump(checkpoint, file, indent=2)
            file.flush()
            os.fsync(file.fileno())

        os.replace(temp_path, path)

    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def mark_videos_ingested(checkpoint, video_ids):
    if not video_ids:
        return checkpoint

    processed = set(checkpoint["processed_video_ids"])
    processed.update(video_ids)

    checkpoint["processed_video_ids"] = sorted(processed)
    checkpoint["last_processed_video_id"] = video_ids[-1]
    checkpoint["status"] = "IN_PROGRESS"

    return checkpoint