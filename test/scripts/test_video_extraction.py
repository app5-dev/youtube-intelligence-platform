
import os
import json
import requests

from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
BASE_URL = "https://www.googleapis.com/youtube/v3"

if not API_KEY:
    raise RuntimeError("Missing YOUTUBE_API_KEY")


def get_json(endpoint, params):
    response = requests.get(
        f"{BASE_URL}/{endpoint}",
        params={**params, "key": API_KEY},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


# Step 1: Resolve the channel
channel_response = get_json(
    "channels",
    {
        "part": "contentDetails",
        "forHandle": "@GoogleDevelopers",
    },
)

channel = channel_response["items"][0]
uploads_id = channel["contentDetails"]["relatedPlaylists"]["uploads"]

print("Uploads playlist:", uploads_id)


# Step 2: Retrieve the first 10 video IDs
def extract_video_ids(uploads_id, max_videos=100):
    video_ids = []
    next_page_token = None

    while len(video_ids) < max_videos:
        remaining = max_videos - len(video_ids)

        params = {
            "part": "contentDetails",
            "playlistId": uploads_id,
            "maxResults": min(50, remaining),
        }

        if next_page_token:
            params["pageToken"] = next_page_token

        response = get_json("playlistItems", params)

        for item in response.get("items", []):
            video_ids.append(
                item["contentDetails"]["videoId"]
            )

        next_page_token = response.get("nextPageToken")

        if not next_page_token:
            break

    return video_ids


video_ids = extract_video_ids(uploads_id, max_videos=100)

print("Total video IDs:", len(video_ids))


# Step 3: Retrieve video metadata in batches

def extract_video_details(video_ids, batch_size=50):
    all_videos = []

    for i in range(0, len(video_ids), batch_size):
        batch_ids = video_ids[i:i + batch_size]

        response = get_json(
            "videos",
            {
                "part": "snippet,statistics,contentDetails",
                "id": ",".join(batch_ids),
            },
        )

        all_videos.extend(response.get("items", []))

        print(
            f"Batch {i // batch_size + 1}: "
            f"{len(response.get('items', []))} videos retrieved"
        )

    return all_videos


videos = extract_video_details(video_ids)

print("Total video details retrieved:", len(videos))

for video in videos:
    print({
        "id": video["id"],
        "title": video["snippet"]["title"],
        "views": video.get("statistics", {}).get("viewCount"),
    })



# Step 4: Save the raw API responses

output_path = Path("data/bronze/youtube/videos")
output_path.mkdir(parents=True, exist_ok=True)

with (output_path / "test_batch.json").open(
    "w", encoding="utf-8"
) as file:
    json.dump(videos, file, ensure_ascii=False, indent=2)

print("Video batch saved successfully")
