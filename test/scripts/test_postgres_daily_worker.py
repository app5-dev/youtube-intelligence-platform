
from pathlib import Path
from tempfile import TemporaryDirectory
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
from src.youtube_intelligence_platform.ingestion.postgres_daily_worker import (
    PostgresDailyVideoWorker,
)
from src.youtube_intelligence_platform.storage.bronze import (
    BronzeStorage,
)


class FakeYouTubeClient:
    def __init__(self):
        self.calls = 0

    def get_video_details(self, video_ids):
        self.calls += 1
        return [
            {
                "id": video_id,
                "snippet": {"title": f"Video {video_id}"},
                "statistics": {"viewCount": "1000"},
            }
            for video_id in video_ids
        ]


channel_id = "UC_TEST_" + uuid4().hex[:12]
ids = ["V501", "V502", "V503"]

queue = PostgresPendingQueue()
registry = PostgresIngestedRegistry()
completion = PostgresIngestionCompletion()

try:
    with TemporaryDirectory() as temp_dir:
        storage = BronzeStorage(Path(temp_dir) / "bronze")
        client = FakeYouTubeClient()

        # Register an isolated test channel.
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

        assert queue.enqueue(channel_id, ids) == 3

        worker = PostgresDailyVideoWorker(
            client=client,
            storage=storage,
            queue=queue,
            registry=registry,
            completion=completion,
        )

        # First execution
        first = worker.run(channel_id)

        assert first["saved"] == 3
        assert first["remaining"] == 0
        assert registry.get_ingested_ids(channel_id) == set(ids)

        bronze_files = list(
            (Path(temp_dir) / "bronze").rglob("batch_*.json")
        )
        assert len(bronze_files) == 1
        assert "initial" in bronze_files[0].parts

        # Repeated discovery must not reinsert completed tasks.
        assert queue.enqueue(channel_id, ids) == 0

        second = worker.run(channel_id)

        assert second["saved"] == 0
        assert second["remaining"] == 0
        assert client.calls == 1

        print("First run saved:", first["saved"])
        print("Second run saved:", second["saved"])
        print("Bronze files:", len(bronze_files))
        print("PostgreSQL daily worker test: PASSED")

finally:
    # Clean up only this test channel.
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM ingestion.ingestion_tasks WHERE channel_id = %s",
                (channel_id,),
            )
            cursor.execute(
                "DELETE FROM ingestion.ingested_videos WHERE channel_id = %s",
                (channel_id,),
            )
            cursor.execute(
                "DELETE FROM ingestion.channels WHERE channel_id = %s",
                (channel_id,),
            )
