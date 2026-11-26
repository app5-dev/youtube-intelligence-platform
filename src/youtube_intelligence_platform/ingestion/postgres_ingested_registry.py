
from src.youtube_intelligence_platform.database.connection import (
    get_db_connection,
)


class PostgresIngestedRegistry:
    #Read ingested IDs for a channel
    def get_ingested_ids(self, channel_id):
        """Return all successfully ingested video IDs."""

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT video_id
                    FROM ingestion.ingested_videos
                    WHERE channel_id = %s
                    """,
                    (channel_id,),
                )

                return {
                    row[0] for row in cursor.fetchall()
                }
    
    # Retrieve a video's Bronze location
    def get_bronze_path(self, channel_id, video_id):
        """Find the Bronze file containing a video."""

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT bronze_path
                    FROM ingestion.ingested_videos
                    WHERE channel_id = %s
                      AND video_id = %s
                    """,
                    (channel_id, video_id),
                )

                row = cursor.fetchone()

                return row[0] if row else None

    # Insert successful ingestion records
    def mark_daily_ingested(
        self,
        channel_id,
        video_ids,
        bronze_path,
    ):
        """Register videos after successful Bronze persistence."""

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

        inserted = 0

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
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
                        RETURNING video_id
                        """,
                        (channel_id, video_id, bronze_path),
                    )

                    if cursor.fetchone() is not None:
                        inserted += 1

        return inserted

