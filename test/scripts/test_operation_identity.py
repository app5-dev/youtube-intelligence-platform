
from src.youtube_intelligence_platform.storage.idempotency import (
    generate_batch_id,
)


channel_id = "UC_TEST"
video_ids = ["V501", "V502"]

# Same logical operation, regardless of video order.
initial_1 = generate_batch_id(
    channel_id,
    video_ids,
    operation="video_initial",
)

initial_2 = generate_batch_id(
    channel_id,
    ["V502", "V501"],
    operation="video_initial",
)

assert initial_1 == initial_2

# Different logical operation.
legacy = generate_batch_id(
    channel_id,
    video_ids,
    operation="historical_backfill",
)

assert initial_1 != legacy

# Different channel: different identity.
other_channel = generate_batch_id(
    "UC_OTHER",
    video_ids,
    operation="video_initial",
)

assert initial_1 != other_channel

print("Order-independent identity: PASSED")
print("Operation isolation: PASSED")
print("Channel isolation: PASSED")
print("Operation identity test: PASSED")
