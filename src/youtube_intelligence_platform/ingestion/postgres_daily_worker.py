
class PostgresDailyVideoWorker:
    def __init__(self, client, storage, queue, registry, completion):
        self.client = client
        self.storage = storage
        self.queue = queue
        self.registry = registry
        self.completion = completion

    def run(self, channel_id, batch_size=50):
        if not 1 <= batch_size <= 50:
            raise ValueError("batch_size must be between 1 and 50")

        pending_ids = self.queue.peek(
            channel_id,
            limit=batch_size,
        )

        if not pending_ids:
            return {"saved": 0, "remaining": 0}

        known_ids = self.registry.get_ingested_ids(channel_id)

        already_ingested = [
            video_id
            for video_id in pending_ids
            if video_id in known_ids
        ]

        # Reconcile tasks for videos already registered.
        if already_ingested:
            self.queue.acknowledge(
                channel_id,
                already_ingested,
            )

        new_ids = [
            video_id
            for video_id in pending_ids
            if video_id not in known_ids
        ]

        if not new_ids:
            return {
                "saved": 0,
                "remaining": len(
                    self.queue.load(channel_id)["pending_video_ids"]
                ),
            }

        videos = self.client.get_video_details(new_ids)

        returned_ids = [video["id"] for video in videos]

        if (
            len(returned_ids) != len(set(returned_ids))
            or not set(returned_ids).issubset(set(new_ids))
        ):
            raise ValueError("Invalid API response video IDs")

        if not videos:
            return {
                "saved": 0,
                "remaining": len(
                    self.queue.load(channel_id)["pending_video_ids"]
                ),
            }

        # Step 1: Persist the raw records.
        path = self.storage.save_videos(
            videos,
            channel_id,
            operation="video_initial",
        )

        # Step 2: Atomically register videos and complete tasks.
        completed = self.completion.complete(
            channel_id,
            returned_ids,
            bronze_path=str(path),
        )

        remaining = len(
            self.queue.load(channel_id)["pending_video_ids"]
        )

        print("Bronze saved:", path)
        print("Tasks completed:", completed)
        print("Remaining pending:", remaining)

        return {
            "saved": completed,
            "remaining": remaining,
        }
