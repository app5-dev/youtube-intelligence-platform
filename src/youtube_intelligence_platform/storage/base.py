
from typing import Protocol


class VideoBronzeStorage(Protocol):
    def save_videos(
        self,
        videos: list[dict],
        channel_id: str,
        operation: str = "historical_backfill",
    ) -> object:
        ...
