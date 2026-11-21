
from src.youtube_intelligence_platform.storage.idempotency import (
    generate_batch_id,
)


# Same videos, different order
ids_a = ["V100", "V101", "V102"]
ids_b = ["V102", "V100", "V101"]

batch_a = generate_batch_id("UC_TEST", ids_a)
batch_b = generate_batch_id("UC_TEST", ids_b)

assert batch_a == batch_b

# Different video set
batch_c = generate_batch_id(
    "UC_TEST",
    ["V100", "V101", "V103"],
)

assert batch_a != batch_c

# Different channel
batch_d = generate_batch_id(
    "UC_OTHER",
    ids_a,
)

assert batch_a != batch_d

# Different ingestion operation
batch_e = generate_batch_id(
    "UC_TEST",
    ids_a,
    operation="statistics_refresh",
)

assert batch_a != batch_e

print("Deterministic batch IDs: PASSED")
print("Same input produces the same ID:", batch_a == batch_b)
print("Different video set produces a different ID:", batch_a != batch_c)
print("Different channel produces a different ID:", batch_a != batch_d)
print("Different operation produces a different ID:", batch_a != batch_e)
