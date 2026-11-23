#Save raw API responses to Bronze storage


import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from src.youtube_intelligence_platform.storage.idempotency import (
    generate_batch_id,
)


class BronzeStorage:
    def __init__(self, base_path="data/bronze/youtube"):
        self.base_path = Path(base_path)

    
    def _validate_existing(
        self,
        destination,
        channel_id,
        video_ids,
        batch_id,
        operation="historical_backfill",
    ):
        with open(destination, "r", encoding="utf-8") as file:
            document = json.load(file)

        metadata = document["metadata"]

        if metadata["channel_id"] != channel_id:
            raise ValueError("Channel ID mismatch")

        if metadata["batch_id"] != batch_id:
            raise ValueError("Batch ID mismatch")

        if metadata["operation"] != operation:
            raise ValueError("Operation mismatch")

        existing_video_ids = [
            video["id"]
            for video in document["raw"]
        ]

        if (
            len(existing_video_ids) != len(set(existing_video_ids))
            or set(existing_video_ids) != set(video_ids)
        ):
            raise ValueError("Video IDs mismatch")

        if metadata["record_count"] != len(existing_video_ids):
            raise ValueError("Record count mismatch")

        return destination


    
    def save_videos(
        self,
        videos,
        channel_id,
        operation="historical_backfill",
    ):
        if not videos:
            raise ValueError("Cannot save an empty batch")

        # 1. Validate the ingestion operation.
        operation_directories = {
            "historical_backfill": "backfill",
            "video_initial": "initial",
        }

        if operation not in operation_directories:
            raise ValueError(
                f"Unsupported operation: {operation}"
            )

        operation_directory = operation_directories[operation]

        video_ids = [video["id"] for video in videos]

        if len(video_ids) != len(set(video_ids)):
            raise ValueError("Duplicate video IDs in batch")

        # 2. Generate an operation-specific batch identity.
        batch_id = generate_batch_id(
            channel_id=channel_id,
            video_ids=video_ids,
            operation=operation,
        )

        # 3. Store different operations in separate directories.
        output_dir = (
            self.base_path
            / "videos"
            / operation_directory
            / f"channel_id={channel_id}"
        )

        output_dir.mkdir(parents=True, exist_ok=True)

        destination = output_dir / f"batch_{batch_id}.json"

        # Retry after an earlier successful write.
        if destination.exists():
            print(f"Reusing existing Bronze batch: {destination}")

            return self._validate_existing(
                destination,
                channel_id,
                video_ids,
                batch_id,
                operation,
            )

        now = datetime.now(timezone.utc)

        document = {
            "metadata": {
                "source": "youtube_data_api_v3",
                "entity": "video",
                "operation": operation,
                "channel_id": channel_id,
                "batch_id": batch_id,
                "ingested_at": now.isoformat(),
                "record_count": len(videos),
            },
            "raw": videos,
        }

        temporary = None

        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=output_dir,
                suffix=".tmp",
                delete=False,
            ) as file:
                temporary = Path(file.name)

                json.dump(
                    document,
                    file,
                    ensure_ascii=False,
                    indent=2,
                )

                file.flush()
                os.fsync(file.fileno())

            # Atomic replacement on the same filesystem.
            os.replace(temporary, destination)

        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

        return destination


