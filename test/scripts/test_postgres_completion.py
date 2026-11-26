
from uuid import uuid4

from src.youtube_intelligence_platform.database.connection import (
    get_db_connection,
)
from src.youtube_intelligence_platform.ingestion.postgres_pending_queue import (
    PostgresPendingQueue,
)
from src.youtube_intelligence_platform.ingestion.postgres_ingested_registry import (
    PostgresIngestedRegistry,
)
from src.youtube_intelligence_platform.ingestion.postgres_completion import (
    PostgresIngestionCompletion,
)


channel_id = "UC_TEST_" + uuid4().hex[:12]
ids = ["V501", "V502"]
bronze_path = "bronze/test/batch_valid.json"

queue = PostgresPendingQueue()
registry = PostgresIngestedRegistry()
completion = PostgresIngestionCompletion()

try:
    # Create isolated test channel.
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO ingestion.channels (
                    channel_id, handle, category
                )
                VALUES (%s, %s, 'test')
                """,
                (channel_id, "@" + channel_id),
            )

    assert queue.enqueue(channel_id, ids) == 2

    # Create an existing registry record with another path.
    # This will deliberately trigger an error during completion.
    registry.mark_daily_ingested(
        channel_id,
        ["V502"],
        "bronze/test/conflicting.json",
    )

    # The first insert (V501) should be rolled back
    # when V502 causes a path mismatch.
    try:
        completion.complete(channel_id, ids, bronze_path)
        raise AssertionError("Expected path mismatch")
    except ValueError as error:
        assert "Bronze path mismatch" in str(error)

    # Verify V501 was NOT committed.
    assert registry.get_bronze_path(channel_id, "V501") is None

    # Both tasks remain PENDING.
    assert queue.peek(channel_id) == ids

    print("Transaction rollback: PASSED")

    # Now complete V501 successfully.
    completed = completion.complete(
        channel_id,
        ["V501"],
        bronze_path,
    )

    assert completed == 1
    assert registry.get_bronze_path(
        channel_id, "V501"
    ) == bronze_path

    assert queue.peek(channel_id) == ["V502"]

    # Complete V502 using its existing valid path.
    completed = completion.complete(
        channel_id,
        ["V502"],
        "bronze/test/conflicting.json",
    )

    assert completed == 1
    assert queue.peek(channel_id) == []

    assert registry.get_ingested_ids(channel_id) == set(ids)

    print("Atomic completion: PASSED")
    print("All PostgreSQL completion tests: PASSED")

finally:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM ingestion.ingestion_tasks "
                "WHERE channel_id = %s",
                (channel_id,),
            )
            cursor.execute(
                "DELETE FROM ingestion.ingested_videos "
                "WHERE channel_id = %s",
                (channel_id,),
            )
            cursor.execute(
                "DELETE FROM ingestion.channels "
                "WHERE channel_id = %s",
                (channel_id,),
            )
