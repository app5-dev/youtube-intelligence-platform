
from src.youtube_intelligence_platform.ingestion.channel_registry import (
    load_channels,
)


channels = load_channels()

assert len(channels) == 3

handles = [channel["handle"] for channel in channels]

assert len(handles) == len(set(handles))

for channel in channels:
    print(
        f"Channel: {channel['handle']} | "
        f"Category: {channel['category']}"
    )

print("Channel registry test: PASSED")
