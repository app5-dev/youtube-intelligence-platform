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
        self, path, channel_id, expected_ids, batch_id
    ):
        """Verify an existing committed backfill batch."""
        with path.open("r", encoding="utf-8") as file:
            document = json.load(file)

        metadata = document["metadata"]
        videos = document["raw"]

        actual_ids = [video["id"] for video in videos]

        if (
            metadata.get("channel_id") != channel_id
            or metadata.get("operation") != "historical_backfill"
            or metadata.get("batch_id") != batch_id
            or len(actual_ids) != len(set(actual_ids))
            or set(actual_ids) != set(expected_ids)
        ):
            raise ValueError(
                f"Existing Bronze batch failed validation: {path}"
            )

        return path

    def save_videos(self, videos, channel_id):
        if not videos:
            raise ValueError("Cannot save an empty batch")

        video_ids = [video["id"] for video in videos]

        if len(video_ids) != len(set(video_ids)):
            raise ValueError("Duplicate video IDs in batch")

        # Stable identity for the same historical batch.
        batch_id = generate_batch_id(
            channel_id,
            video_ids,
            operation="historical_backfill",
        )

        # Stable across calendar days.
        output_dir = (
            self.base_path
            / "videos"
            / "backfill"
            / f"channel_id={channel_id}"
        )
        output_dir.mkdir(parents=True, exist_ok=True)

        destination = output_dir / f"batch_{batch_id}.json"

        # Retry after an earlier successful write.
        if destination.exists():
            print(f"Reusing existing Bronze batch: {destination}")
            return self._validate_existing(
                destination, channel_id, video_ids, batch_id
            )

        now = datetime.now(timezone.utc)

        document = {
            "metadata": {
                "source": "youtube_data_api_v3",
                "entity": "video",
                "operation": "historical_backfill",
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

