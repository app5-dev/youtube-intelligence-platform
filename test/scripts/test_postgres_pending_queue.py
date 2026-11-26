
from uuid import uuid4

from src.youtube_intelligence_platform.database.connection import (
    get_db_connection,
)
from src.youtube_intelligence_platform.ingestion.postgres_pending_queue import (
    PostgresPendingQueue,
)


channel_id = "UC_TEST_" + uuid4().hex[:12]
video_ids = ["V501", "V502", "V503"]

queue = PostgresPendingQueue()

try:
    # Create a temporary test channel.
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO ingestion.channels (
                    channel_id, handle, category
                )
                VALUES (%s, %s, %s)
                """,
                (channel_id, "@" + channel_id, "test"),
            )

    # First insertion
    added = queue.enqueue(channel_id, video_ids)
    assert added == 3

    # Repeated insertion
    added_again = queue.enqueue(channel_id, video_ids)
    assert added_again == 0

    # Read without removing
    pending = queue.peek(channel_id)
    assert pending == video_ids

    # Mark two tasks completed
    completed = queue.acknowledge(
        channel_id, ["V501", "V502"]
    )

    assert completed == 2
    assert queue.peek(channel_id) == ["V503"]

    print("First insertion:", added)
    print("Duplicate insertion:", added_again)
    print("Completed tasks:", completed)
    print("Remaining:", queue.peek(channel_id))
    print("PostgreSQL pending queue test: PASSED")

finally:
    # Clean up the temporary test records.
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM ingestion.ingestion_tasks
                WHERE channel_id = %s
                """,
                (channel_id,),
            )
            cursor.execute(
                """
                DELETE FROM ingestion.channels
                WHERE channel_id = %s
                """,
                (channel_id,),
            )
