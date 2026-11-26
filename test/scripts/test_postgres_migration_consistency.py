
from pathlib import Path

from src.youtube_intelligence_platform.ingestion.ingested_registry import (
    IngestedVideoRegistry,
)
from src.youtube_intelligence_platform.ingestion.postgres_ingested_registry import (
    PostgresIngestedRegistry,
)


# Verified channel IDs
channels = {
    "@GoogleDevelopers": "UC_x5XG1OV2P6uZZ5FSM9Ttw",
    "@TED": "UCAuUUnT6oDeKwE6v1NGQxug",
    "@NASA": "UCLA_DiR1FfKNvjuUpBHmylQ",
}

local_registry = IngestedVideoRegistry(
    checkpoint_dir="data/checkpoints"
)

postgres_registry = PostgresIngestedRegistry()

total = 0

for handle, channel_id in channels.items():

    local_ids = local_registry.get_ingested_ids(
        channel_id
    )

    postgres_ids = postgres_registry.get_ingested_ids(
        channel_id
    )

    missing = local_ids - postgres_ids
    extra = postgres_ids - local_ids

    print(f"\nChannel: {handle}")
    print("Local IDs:", len(local_ids))
    print("PostgreSQL IDs:", len(postgres_ids))
    print("Missing in PostgreSQL:", len(missing))
    print("Extra in PostgreSQL:", len(extra))

    assert not missing, f"Missing IDs: {sorted(missing)[:5]}"
    assert not extra, f"Unexpected IDs: {sorted(extra)[:5]}"

    total += len(postgres_ids)

print("\nTotal verified videos:", total)
print("PostgreSQL migration consistency: PASSED")
