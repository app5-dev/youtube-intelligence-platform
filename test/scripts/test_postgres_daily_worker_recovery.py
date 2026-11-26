
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
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


def run_recovery_test(failure_point):
    channel_id = "UC_TEST_" + uuid4().hex[:12]
    video_ids = ["V501", "V502", "V503"]

    queue = PostgresPendingQueue()
    registry = PostgresIngestedRegistry()
    completion = PostgresIngestionCompletion()
    client = FakeYouTubeClient()

    try:
        with TemporaryDirectory() as temp_dir:
            bronze_dir = Path(temp_dir) / "bronze"
            storage = BronzeStorage(bronze_dir)

            # Create a dedicated test channel.
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

            queue.enqueue(channel_id, video_ids)

            worker = PostgresDailyVideoWorker(
                client=client,
                storage=storage,
                queue=queue,
                registry=registry,
                completion=completion,
            )

            original_complete = completion.complete

            if failure_point == "before_commit":
                # Simulates a failure before transaction completion.
                def fail_completion(*args, **kwargs):
                    raise OSError(
                        "Simulated PostgreSQL completion failure"
                    )

            elif failure_point == "after_commit":
                # Commit normally, then simulate an error
                # before the worker receives confirmation.
                def fail_completion(*args, **kwargs):
                    original_complete(*args, **kwargs)
                    raise OSError(
                        "Simulated failure after database commit"
                    )

            else:
                raise ValueError("Unknown failure point")

            # First execution must fail.
            with patch.object(
                completion,
                "complete",
                side_effect=fail_completion,
            ):
                try:
                    worker.run(channel_id)
                    raise AssertionError(
                        "Expected simulated failure"
                    )
                except OSError as error:
                    print("Expected failure:", error)

            # Bronze persistence happened before the failure.
            files = list(bronze_dir.rglob("batch_*.json"))
            assert len(files) == 1
            assert "initial" in files[0].parts

            if failure_point == "before_commit":
                # No tasks completed.
                assert queue.peek(channel_id) == video_ids
                assert registry.get_ingested_ids(
                    channel_id
                ) == set()

                # Retry should reuse the existing Bronze file.
                result = worker.run(channel_id)

                assert result["saved"] == 3
                assert client.calls == 2

            else:
                # Transaction was already committed.
                assert queue.peek(channel_id) == []
                assert registry.get_ingested_ids(
                    channel_id
                ) == set(video_ids)

                # Retry must not ingest again.
                result = worker.run(channel_id)

                assert result["saved"] == 0
                assert client.calls == 1

            # Final state must be consistent.
            assert queue.peek(channel_id) == []
            assert registry.get_ingested_ids(
                channel_id
            ) == set(video_ids)

            assert len(
                list(bronze_dir.rglob("batch_*.json"))
            ) == 1

            print(f"{failure_point.upper()}: PASSED")

    finally:
        # Clean up this test channel only.
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


run_recovery_test("before_commit")
run_recovery_test("after_commit")

print("\nPostgreSQL worker recovery tests: PASSED")
