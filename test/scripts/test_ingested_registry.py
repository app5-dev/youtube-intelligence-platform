
from pathlib import Path
from tempfile import TemporaryDirectory

from src.youtube_intelligence_platform.checkpoint import (
    load_checkpoint,
    save_checkpoint,
    mark_videos_ingested,
)

from src.youtube_intelligence_platform.ingestion.ingested_registry import (
    IngestedVideoRegistry,
)


with TemporaryDirectory() as temp_dir:
    root = Path(temp_dir)
    channel_id = "UC_TEST"

    # Simulate completed historical backfill.
    backfill_path = root / f"{channel_id}_backfill.json"

    checkpoint = load_checkpoint(backfill_path, channel_id)
    mark_videos_ingested(
        checkpoint, ["V100", "V101", "V102"]
    )
    save_checkpoint(backfill_path, checkpoint)

    registry = IngestedVideoRegistry(root)

    known = registry.get_ingested_ids(channel_id)

    assert known == {"V100", "V101", "V102"}

    # Simulate successful daily ingestion.
    registry.mark_daily_ingested(
        channel_id, ["V103", "V104"]
    )

    # A retry must not create duplicates.
    registry.mark_daily_ingested(
        channel_id, ["V104"]
    )

    # Simulate a pipeline restart.
    restarted = IngestedVideoRegistry(root)
    known = restarted.get_ingested_ids(channel_id)

    assert known == {
        "V100", "V101", "V102", "V103", "V104"
    }

    print("Historical videos:", 3)
    print("Daily videos:", 2)
    print("Total unique ingested:", len(known))
    print("Shared registry test: PASSED")
