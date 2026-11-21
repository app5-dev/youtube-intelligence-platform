
import hashlib
import json


def generate_batch_id(
    channel_id: str,
    video_ids: list[str],
    operation: str = "historical_backfill",
) -> str:
    if not channel_id:
        raise ValueError("channel_id cannot be empty")

    if not video_ids:
        raise ValueError("video_ids cannot be empty")

    if not operation:
        raise ValueError("operation cannot be empty")

    # Canonical representation independent of input ordering
    identity = {
        "operation": operation,
        "channel_id": channel_id,
        "video_ids": sorted(set(video_ids)),
    }

    payload = json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()
