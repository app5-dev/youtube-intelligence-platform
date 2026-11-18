#Save raw API responses to Bronze storage


import json
import uuid
from datetime import datetime, timezone
from pathlib import Path


class BronzeStorage:
    def __init__(self, base_path="data/bronze/youtube"):
        self.base_path = Path(base_path)

    def save_videos(self, videos, channel_id):
        if not videos:
            raise ValueError("Cannot save an empty batch")

        now = datetime.now(timezone.utc)
        ingestion_date = now.strftime("%Y-%m-%d")
        run_id = uuid.uuid4().hex

        output_dir = (
            self.base_path
            / "videos"
            / f"ingestion_date={ingestion_date}"
            / f"channel_id={channel_id}"
        )

        output_dir.mkdir(parents=True, exist_ok=True)

        destination = output_dir / f"batch_{run_id}.json"
        temporary = destination.with_suffix(".tmp")

        document = {
            "metadata": {
                "source": "youtube_data_api_v3",
                "entity": "video",
                "channel_id": channel_id,
                "ingested_at": now.isoformat(),
                "run_id": run_id,
                "record_count": len(videos),
            },
            "raw": videos,
        }

        try:
            with temporary.open("w", encoding="utf-8") as file:
                json.dump(
                    document,
                    file,
                    ensure_ascii=False,
                    indent=2,
                )

            temporary.replace(destination)

        finally:
            temporary.unlink(missing_ok=True)

        return destination
