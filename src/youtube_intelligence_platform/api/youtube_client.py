#Communicate with YouTube API, handle pagination and batching


import os

import requests
from dotenv import load_dotenv

import logging

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.googleapis.com/youtube/v3"


class YouTubeClient:
    def __init__(self, api_key=None):
        load_dotenv()

        self.api_key = api_key or os.getenv("YOUTUBE_API_KEY")

        if not self.api_key:
            raise ValueError("Missing YOUTUBE_API_KEY")

        self.session = requests.Session()

        retry_policy = Retry(
            total=4,
            connect=4,
            read=2,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(max_retries=retry_policy)

        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)


    def get_json(self, endpoint, params):
        response = self.session.get(
            f"{BASE_URL}/{endpoint}",
            params={
                **params,
                "key": self.api_key,
            },
            timeout=30,
        )

        response.raise_for_status()
        return response.json()

    def get_channel(self, handle):
        response = self.get_json(
            "channels",
            {
                "part": "contentDetails,snippet,statistics",
                "forHandle": handle,
            },
        )

        items = response.get("items", [])

        if not items:
            raise ValueError(f"Channel not found: {handle}")

        return items[0]

    def get_playlist_page(self, playlist_id, page_token=None):
        params = {
            "part": "contentDetails",
            "playlistId": playlist_id,
            "maxResults": 50,
        }

        if page_token:
            params["pageToken"] = page_token

        return self.get_json("playlistItems", params)

    def get_video_details(self, video_ids):
        if not video_ids:
            return []

        if len(video_ids) > 50:
            raise ValueError(
                "Maximum 50 video IDs per request"
            )

        response = self.get_json(
            "videos",
            {
                "part": "snippet,statistics,contentDetails",
                "id": ",".join(video_ids),
            },
        )

        return response.get("items", [])

    def close(self):
        self.session.close()
