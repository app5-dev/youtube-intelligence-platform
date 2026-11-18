
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

import requests
from dotenv import load_dotenv

from src.youtube_intelligence_platform.api.youtube_client import (
    YouTubeClient,
)
from src.youtube_intelligence_platform.checkpoint import (
    load_checkpoint,
    save_checkpoint,
    mark_videos_ingested,
)
from src.youtube_intelligence_platform.storage.bronze import (
    BronzeStorage,
)

# Backfill configuration
CHANNEL_HANDLE = "@GoogleDevelopers"
MAX_VIDEOS_PER_RUN = 100
def discover_video_ids(
    client,
    playlist_id,
    processed_ids,
    limit,
):
    new_ids = []
    page_token = None
    seen = set()

    while len(new_ids) < limit:
        response = client.get_playlist_page(
            playlist_id,
            page_token,
        )

        for item in response.get("items", []):
            video_id = item["contentDetails"]["videoId"]

            if video_id in seen:
                continue

            seen.add(video_id)

            if video_id not in processed_ids:
                new_ids.append(video_id)

            if len(new_ids) >= limit:
                break

        page_token = response.get("nextPageToken")

        if not page_token:
            break

    return new_ids



def save_bronze(videos, channel_id):
    now = datetime.now(timezone.utc)
    date = now.strftime("%Y-%m-%d")
    run_id = uuid.uuid4().hex

    directory = (
        Path("data/bronze/youtube/videos")
        / f"ingestion_date={date}"
        / f"channel_id={channel_id}"
    )
    directory.mkdir(parents=True, exist_ok=True)

    destination = directory / f"batch_{run_id}.json"
    temporary = destination.with_suffix(".tmp")

    document = {
        "metadata": {
            "source": "youtube_data_api_v3",
            "channel_id": channel_id,
            "ingested_at": now.isoformat(),
            "run_id": run_id,
        },
        "raw": videos,
    }

    try:
        with temporary.open("w", encoding="utf-8") as file:
            json.dump(document, file, ensure_ascii=False, indent=2)

        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)

    return destination



def main():
    client = YouTubeClient()
    storage = BronzeStorage()

    try:
        channel = client.get_channel(CHANNEL_HANDLE)

        channel_id = channel["id"]
        uploads_id = channel[
            "contentDetails"
        ]["relatedPlaylists"]["uploads"]

        checkpoint_path = (
            Path("data/checkpoints")
            / f"{channel_id}_backfill.json"
        )

        checkpoint = load_checkpoint(
            checkpoint_path,
            channel_id,
        )

        processed_ids = set(
            checkpoint["processed_video_ids"]
        )

        print("Previously ingested:", len(processed_ids))

        new_ids = discover_video_ids(
            client,
            uploads_id,
            processed_ids,
            MAX_VIDEOS_PER_RUN,
        )

        print("Videos selected:", len(new_ids))

        for start in range(0, len(new_ids), 50):
            batch_ids = new_ids[start:start + 50]

            videos = client.get_video_details(batch_ids)

            if not videos:
                print("Empty batch; not checkpointed")
                continue

            path = storage.save_videos(videos, channel_id)

            completed_ids = [
                video["id"]
                for video in videos
            ]

            mark_videos_ingested(
                checkpoint,
                completed_ids,
            )

            save_checkpoint(
                checkpoint_path,
                checkpoint,
            )

            print(
                f"Saved {len(completed_ids)} "
                f"videos -> {path}"
            )

        print(
            "Total checkpointed:",
            len(checkpoint["processed_video_ids"]),
        )

    finally:
        client.close()


if __name__ == "__main__":
    main()
