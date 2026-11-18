
from src.youtube_intelligence_platform.api.youtube_client import (
    YouTubeClient,
)


client = YouTubeClient()

try:
    channel = client.get_channel("@GoogleDevelopers")

    print("Channel:", channel["snippet"]["title"])
    print("Channel ID:", channel["id"])

    uploads_id = channel["contentDetails"]["relatedPlaylists"]["uploads"]

    page = client.get_playlist_page(uploads_id)

    video_ids = [
        item["contentDetails"]["videoId"]
        for item in page["items"]
    ]

    print("Video IDs retrieved:", len(video_ids))

    videos = client.get_video_details(video_ids[:5])

    print("Video details retrieved:", len(videos))

    assert len(videos) == 5

    print("YouTube client integration test: PASSED")

finally:
    client.close()
