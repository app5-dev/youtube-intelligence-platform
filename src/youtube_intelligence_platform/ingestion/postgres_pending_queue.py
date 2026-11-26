
from src.youtube_intelligence_platform.database.connection import (
    get_db_connection,
)


class PostgresPendingQueue:
    def enqueue(self, channel_id, video_ids):
        """Insert initial-ingestion tasks without duplicates."""

        if not video_ids:
            return 0

        if any(
            not isinstance(video_id, str) or not video_id
            for video_id in video_ids
        ):
            raise ValueError("Invalid video ID")

        unique_ids = list(dict.fromkeys(video_ids))
        added = 0

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                for video_id in unique_ids:
                    cursor.execute(
                        """
                        INSERT INTO ingestion.ingestion_tasks (
                            channel_id,
                            video_id,
                            operation,
                            scope_key,
                            status
                        )
                        VALUES (%s, %s, %s, %s, 'PENDING')
                        ON CONFLICT (
                            channel_id,
                            video_id,
                            operation,
                            scope_key
                        )
                        DO NOTHING
                        RETURNING task_id
                        """,
                        (
                            channel_id,
                            video_id,
                            "video_initial",
                            "initial",
                        ),
                    )

                    if cursor.fetchone() is not None:
                        added += 1

        return added

    def peek(self, channel_id, limit=50):
        """Read pending tasks without completing them."""

        if limit < 1:
            raise ValueError("limit must be positive")

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT video_id
                    FROM ingestion.ingestion_tasks
                    WHERE channel_id = %s
                      AND operation = 'video_initial'
                      AND scope_key = 'initial'
                      AND status = 'PENDING'
                    ORDER BY created_at, task_id
                    LIMIT %s
                    """,
                    (channel_id, limit),
                )

                return [
                    row[0] for row in cursor.fetchall()
                ]

    def acknowledge(self, channel_id, completed_ids):
        """Mark tasks complete after successful ingestion."""

        completed_ids = list(dict.fromkeys(completed_ids))

        if not completed_ids:
            return 0

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
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
                    (channel_id, completed_ids),
                )

                return cursor.rowcount

    def load(self, channel_id):
        """Compatibility with the current JSON queue interface."""

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT video_id
                    FROM ingestion.ingestion_tasks
                    WHERE channel_id = %s
                      AND operation = 'video_initial'
                      AND scope_key = 'initial'
                      AND status = 'PENDING'
                    ORDER BY created_at, task_id
                    """,
                    (channel_id,),
                )

                pending_ids = [
                    row[0] for row in cursor.fetchall()
                ]

        return {
            "channel_id": channel_id,
            "pending_video_ids": pending_ids,
        }
