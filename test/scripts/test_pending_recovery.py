
from tempfile import TemporaryDirectory

from src.youtube_intelligence_platform.ingestion.pending_queue import (
    PendingVideoQueue,
)


with TemporaryDirectory() as temp_dir:
    channel_id = "UC_TEST"

    queue = PendingVideoQueue(temp_dir)
    queue.enqueue(channel_id, ["V501", "V502", "V503"])

    # First execution
    batch = queue.peek(channel_id)

    print("First attempt:", batch)

    try:
        # Simulate a failure BEFORE successful persistence.
        raise OSError("Simulated ingestion failure")
    except OSError as exc:
        print("Expected failure:", exc)

    # No acknowledgment took place.
    assert queue.peek(channel_id) == batch

    # New execution / new queue instance
    recovered_queue = PendingVideoQueue(temp_dir)

    recovered_batch = recovered_queue.peek(channel_id)

    print("Recovered for retry:", recovered_batch)

    assert recovered_batch == batch

    # Simulate successful Bronze and checkpoint persistence.
    # Real pipeline will perform these operations before ACK.
    bronze_saved = True
    checkpoint_saved = True

    if bronze_saved and checkpoint_saved:
        recovered_queue.acknowledge(
            channel_id, recovered_batch
        )

    assert recovered_queue.peek(channel_id) == []

    print("Pending after successful processing:", 0)
    print("Pending recovery test: PASSED")
