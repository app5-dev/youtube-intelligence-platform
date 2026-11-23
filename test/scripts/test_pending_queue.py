
from tempfile import TemporaryDirectory

from src.youtube_intelligence_platform.ingestion.pending_queue import (
    PendingVideoQueue,
)


with TemporaryDirectory() as temp_dir:
    queue = PendingVideoQueue(base_path=temp_dir)

    channel_id = "UC_TEST"

    # First discovery
    added = queue.enqueue(
        channel_id,
        ["V501", "V502", "V503"],
    )

    assert added == 3
    assert queue.peek(channel_id) == [
        "V501", "V502", "V503"
    ]

    print("First discovery:", queue.peek(channel_id))

    # Same discovery repeated
    added_again = queue.enqueue(
        channel_id,
        ["V501", "V502", "V503"],
    )

    assert added_again == 0
    print("Duplicate IDs added:", added_again)

    # Simulate a new process reading persisted state
    restarted_queue = PendingVideoQueue(
        base_path=temp_dir
    )

    assert len(restarted_queue.peek(channel_id)) == 3

    print("Recovered pending IDs:", restarted_queue.peek(channel_id))

    # Successfully ingested first two videos
    removed = restarted_queue.acknowledge(
        channel_id,
        ["V501", "V502"],
    )

    assert removed == 2
    assert restarted_queue.peek(channel_id) == ["V503"]

    print("Remaining after acknowledgment:",
          restarted_queue.peek(channel_id))

    print("Pending queue test: PASSED")
