
from pathlib import Path

import yaml


def load_channels(config_path="config/channels.yml"):
    path = Path(config_path)

    with path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError("Invalid channel configuration")

    channels = config.get("channels")

    if not isinstance(channels, list):
        raise ValueError("'channels' must be a list")

    enabled_channels = []
    seen_handles = set()

    for channel in channels:
        if not isinstance(channel, dict):
            raise ValueError("Each channel must be an object")

        handle = channel.get("handle")
        category = channel.get("category")

        if not isinstance(handle, str) or not handle.startswith("@"):
            raise ValueError(f"Invalid channel handle: {handle}")

        if not isinstance(category, str) or not category.strip():
            raise ValueError(f"Invalid category for {handle}")

        normalized_handle = handle.lower()

        if normalized_handle in seen_handles:
            raise ValueError(f"Duplicate channel: {handle}")

        seen_handles.add(normalized_handle)

        if channel.get("enabled", True):
            enabled_channels.append(channel)

    return enabled_channels
