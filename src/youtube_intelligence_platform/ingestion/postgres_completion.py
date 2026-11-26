
from src.youtube_intelligence_platform.database.connection import (
    get_db_connection,
)


class PostgresIngestionCompletion:
    def complete(self, channel_id, video_ids, bronze_path):
        """
        Atomically:
        1. Register successfully ingested videos.
        2. Mark their pending tasks as COMPLETED.
        """

        if not isinstance(channel_id, str) or not channel_id:
            raise ValueError("Invalid channel ID")

        if not isinstance(bronze_path, str) or not bronze_path:
            raise ValueError("Bronze path is required")

        if any(
            not isinstance(video_id, str) or not video_id
            for video_id in video_ids
        ):
            raise ValueError("Invalid video ID")

        unique_ids = list(dict.fromkeys(video_ids))

        if not unique_ids:
            return 0

        with get_db_connection() as conn:
            with conn.cursor() as cursor:

                # 1. Lock tasks and verify they are pending.
                cursor.execute(
                    """
                    SELECT video_id, status
                    FROM ingestion.ingestion_tasks
                    WHERE channel_id = %s
                      AND video_id = ANY(%s)
                      AND operation = 'video_initial'
                      AND scope_key = 'initial'
                    FOR UPDATE
                    """,
                    (channel_id, unique_ids),
                )

                tasks = dict(cursor.fetchall())

                if (
                    len(tasks) != len(unique_ids)
                    or any(
                        status != "PENDING"
                        for status in tasks.values()
                    )
                ):
                    raise ValueError(
                        "All videos must have PENDING tasks"
                    )

                # 2. Insert ingestion records.
                for video_id in unique_ids:
                    cursor.execute(
                        """
                        INSERT INTO ingestion.ingested_videos (
                            channel_id,
                            video_id,
                            bronze_path
                        )
                        VALUES (%s, %s, %s)
                        ON CONFLICT (channel_id, video_id)
                        DO NOTHING
                        """,
                        (channel_id, video_id, bronze_path),
                    )

                    # Detect conflicting Bronze locations.
                    cursor.execute(
                        """
                        SELECT bronze_path
                        FROM ingestion.ingested_videos
                        WHERE channel_id = %s
                          AND video_id = %s
                        """,
                        (channel_id, video_id),
                    )

                    stored_path = cursor.fetchone()[0]

                    if stored_path != bronze_path:
                        raise ValueError(
                            f"Bronze path mismatch for {video_id}"
                        )

                # 3. Complete tasks in the SAME transaction.
                cursor.execute(
                    """
                    UPDATE ingestion.ingestion_tasks
                    SET status = 'COMPLETED',
                        completed_at = NOW(),
                        updated_at = NOW()
                    WHERE channel_id = %s
                      AND video_id = ANY(%s)
                      AND operation = 'video_initial'
                      AND scope_key = 'initial'
                      AND status = 'PENDING'
                    """,
                    (channel_id, unique_ids),
                )

                completed = cursor.rowcount

                if completed != len(unique_ids):
                    raise RuntimeError(
                        "Not all tasks were completed"
                    )

        # Exiting the connection context commits
        # only if no exception occurred.
        return completed
