
from uuid import uuid4

from src.youtube_intelligence_platform.database.connection import (
    get_db_connection,
)
from src.youtube_intelligence_platform.ingestion.postgres_ingested_registry import (
    PostgresIngestedRegistry,
)


channel_id = "UC_TEST_" + uuid4().hex[:12]
video_ids = ["V501", "V502", "V503"]

bronze_path = (
    "data/bronze/youtube/videos/initial/"
    f"channel_id={channel_id}/batch_test.json"
)

registry = PostgresIngestedRegistry()

try:
    # 1. Create a temporary test channel.
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO ingestion.channels (
                    channel_id,
                    handle,
                    category
                )
                VALUES (%s, %s, %s)
                """,
                (
                    channel_id,
                    "@" + channel_id,
                    "test",
                ),
            )

    # 2. Check that the registry starts empty.
    assert registry.get_ingested_ids(channel_id) == set()

    # 3. Register successfully ingested videos.
    inserted = registry.mark_daily_ingested(
        channel_id,
        video_ids,
        bronze_path,
    )

    assert inserted == 3

    # 4. Verify stored IDs.
    known_ids = registry.get_ingested_ids(channel_id)

    assert known_ids == set(video_ids)

    # 5. Verify Bronze path.
    saved_path = registry.get_bronze_path(
        channel_id,
        "V501",
    )

    assert saved_path == bronze_path

    # 6. Retry the same operation.
    duplicate_inserted = registry.mark_daily_ingested(
        channel_id,
        video_ids,
        bronze_path,
    )

    assert duplicate_inserted == 0

    # 7. Verify persistence using another instance.
    restarted_registry = PostgresIngestedRegistry()

    assert restarted_registry.get_ingested_ids(
        channel_id
    ) == set(video_ids)

    print("First insertion:", inserted)
    print("Duplicate insertion:", duplicate_inserted)
    print("Total registered:", len(known_ids))
    print("Bronze path verified:", saved_path == bronze_path)
    print("PostgreSQL ingested registry test: PASSED")

finally:
    # Remove only the temporary test records.
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM ingestion.ingested_videos
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
