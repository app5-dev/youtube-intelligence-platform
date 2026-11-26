
import json
from collections import defaultdict
from pathlib import Path


CHECKPOINT_DIR = Path("data/checkpoints")
BRONZE_DIR = Path("data/bronze")


def main():
    # Channel -> IDs recorded as ingested
    checkpointed = defaultdict(set)

    # Channel -> ID -> Bronze file locations
    bronze_inventory = defaultdict(
        lambda: defaultdict(set)
    )

    # 1. Read historical backfill checkpoints.
    for path in CHECKPOINT_DIR.glob("*_backfill.json"):
        with path.open(encoding="utf-8") as file:
            document = json.load(file)

        channel_id = document["channel_id"]

        checkpointed[channel_id].update(
            document.get("processed_video_ids", [])
        )

    # 2. Read the daily ingestion registries.
    daily_dir = CHECKPOINT_DIR / "daily_ingested"

    for path in daily_dir.glob("*.json"):
        with path.open(encoding="utf-8") as file:
            document = json.load(file)

        channel_id = document["channel_id"]

        checkpointed[channel_id].update(
            document.get("video_ids", [])
        )

    # 3. Inspect actual persisted Bronze batches.
    for path in BRONZE_DIR.rglob("batch_*.json"):
        with path.open(encoding="utf-8") as file:
            document = json.load(file)

        channel_id = document["metadata"]["channel_id"]

        for video in document.get("raw", []):
            bronze_inventory[channel_id][
                video["id"]
            ].add(str(path))

    # 4. Compare registry IDs against Bronze files.
    all_channels = sorted(
        set(checkpointed) | set(bronze_inventory)
    )

    total_known = 0
    total_missing = 0

    for channel_id in all_channels:
        known = checkpointed[channel_id]
        stored = set(bronze_inventory[channel_id])

        missing = known - stored
        unregistered = stored - known

        total_known += len(known)
        total_missing += len(missing)

        print("\nChannel:", channel_id)
        print("Registered IDs:", len(known))
        print("Bronze video IDs:", len(stored))
        print("Missing in Bronze:", len(missing))
        print("Not registered:", len(unregistered))

        if missing:
            print("Missing examples:", sorted(missing)[:5])

        if unregistered:
            print(
                "Unregistered examples:",
                sorted(unregistered)[:5],
            )

    print("\n========== AUDIT SUMMARY ==========")
    print("Registered IDs:", total_known)
    print("Missing Bronze records:", total_missing)
    print("Legacy audit completed (read-only)")


if __name__ == "__main__":
    main()
