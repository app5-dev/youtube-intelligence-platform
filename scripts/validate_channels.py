
import json
from pathlib import Path

base = Path("data/bronze/youtube/channels")
files = list(base.rglob("*.json"))

assert files, "No ingested files found"

channel_ids = set()

for path in files:
    with path.open(encoding="utf-8") as file:
        document = json.load(file)

    metadata = document["metadata"]
    channel = document["raw"]["items"][0]

    assert metadata["channel_id"] == channel["id"]
    assert metadata["ingested_at"]
    assert metadata["run_id"]
    assert channel["snippet"]["title"]

    channel_ids.add(channel["id"])

print(f"Validated {len(files)} ingestion files")
print(f"Unique channels: {len(channel_ids)}")
