
import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

from src.youtube_intelligence_platform.database.connection import (
    get_db_connection,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CHECKPOINT_DIR = PROJECT_ROOT / "data" / "checkpoints"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"

SUPPORTED_OPERATIONS = {
    "legacy_date_partitioned": "ingestion_date=",
    "historical_backfill": "backfill",
    "video_initial": "initial",
}

OPERATION_PRIORITY = {
    "legacy_date_partitioned": 1,
    "historical_backfill": 2,
    "video_initial": 3,
}


def read_json(path):
    """Read JSON and report the offending file on failure."""
    try:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"Cannot read JSON file {path}: {exc}"
        ) from exc


def validate_ids(video_ids, path, field_name):
    """Validate a collection of video IDs."""
    if not isinstance(video_ids, list):
        raise ValueError(
            f"Invalid {field_name} in {path}: expected a list"
        )

    for video_id in video_ids:
        if not isinstance(video_id, str) or not video_id:
            raise ValueError(
                f"Invalid video ID in {field_name}: {path}"
            )


def load_registered_ids():
    """
    Collect IDs recorded as successfully ingested.

    Sources:
    - Historical backfill checkpoints
    - Daily ingestion registry files
    """
    registered = defaultdict(set)

    # Historical checkpoints
    for path in sorted(CHECKPOINT_DIR.glob("*_backfill.json")):
        document = read_json(path)

        channel_id = document["channel_id"]
        video_ids = document.get("processed_video_ids", [])

        validate_ids(
            video_ids,
            path,
            "processed_video_ids",
        )

        registered[channel_id].update(video_ids)

    # Daily registry
    daily_dir = CHECKPOINT_DIR / "daily_ingested"

    for path in sorted(daily_dir.glob("*.json")):
        document = read_json(path)

        channel_id = document["channel_id"]
        video_ids = document.get("video_ids", [])

        validate_ids(
            video_ids,
            path,
            "video_ids",
        )

        registered[channel_id].update(video_ids)

    return registered


def detect_operation(path, metadata):
    """
    Detect the logical operation from metadata or
    the recognized legacy directory structure.
    """
    operation = metadata.get("operation")

    # Identify exactly which supported layout is used.
    date_parts = [
        part
        for part in path.parts
        if part.startswith("ingestion_date=")
    ]

    is_legacy_date_partitioned = bool(date_parts)
    is_backfill = "backfill" in path.parts
    is_initial = "initial" in path.parts

    matched_layouts = sum([
        is_legacy_date_partitioned,
        is_backfill,
        is_initial,
    ])

    if matched_layouts != 1:
        raise ValueError(
            f"Ambiguous or unsupported Bronze layout: {path}"
        )

    if is_legacy_date_partitioned:
        if len(date_parts) != 1:
            raise ValueError(
                f"Multiple ingestion date partitions: {path}"
            )

        partition_date = date_parts[0].split("=", 1)[1]

        if not re.fullmatch(
            r"\d{4}-\d{2}-\d{2}",
            partition_date,
        ):
            raise ValueError(
                f"Invalid ingestion_date partition: {path}"
            )

    if operation is None:
        if is_legacy_date_partitioned:
            operation = "legacy_date_partitioned"
        elif is_backfill:
            operation = "historical_backfill"
        elif is_initial:
            operation = "video_initial"

    if operation not in SUPPORTED_OPERATIONS:
        raise ValueError(
            f"Unsupported operation {operation!r} in {path}"
        )

    operation_matches_layout = {
        "legacy_date_partitioned": is_legacy_date_partitioned,
        "historical_backfill": is_backfill,
        "video_initial": is_initial,
    }

    if not operation_matches_layout[operation]:
        raise ValueError(
            f"Operation/path mismatch in {path}: "
            f"operation={operation!r}"
        )

    return operation


def build_bronze_inventory():
    """
    Build:
        inventory[channel_id][video_id] = (
            relative_bronze_path,
            operation,
        )

    Validate every scanned Bronze batch.
    """
    inventory = defaultdict(dict)

    batch_count = 0
    legacy_count = 0
    duplicate_references = 0

    for path in sorted(BRONZE_DIR.rglob("batch_*.json")):
        document = read_json(path)

        if not isinstance(document, dict):
            raise ValueError(
                f"Invalid Bronze document: {path}"
            )

        metadata = document.get("metadata")
        videos = document.get("raw")

        if not isinstance(metadata, dict):
            raise ValueError(
                f"Missing or invalid metadata in {path}"
            )

        if not isinstance(videos, list):
            raise ValueError(
                f"Missing or invalid raw records in {path}"
            )

        channel_id = metadata.get("channel_id")

        if not isinstance(channel_id, str) or not channel_id:
            raise ValueError(
                f"Invalid channel_id in {path}"
            )

        # Validate partition channel against metadata.
        channel_parts = [
            part
            for part in path.parts
            if part.startswith("channel_id=")
        ]

        if len(channel_parts) != 1:
            raise ValueError(
                f"Missing or ambiguous channel partition: {path}"
            )

        path_channel_id = channel_parts[0].split("=", 1)[1]

        if path_channel_id != channel_id:
            raise ValueError(
                f"Channel mismatch in {path}: "
                f"path={path_channel_id}, "
                f"metadata={channel_id}"
            )

        if "operation" not in metadata:
            legacy_count += 1

        operation = detect_operation(path, metadata)


        record_count = metadata.get("record_count")

        if record_count is None:
            if operation != "legacy_date_partitioned":
                raise ValueError(
                    f"Missing record_count in modern Bronze batch: {path}"
                )

            # Legacy batches did not necessarily record this field.
            # Validate the actual raw records instead.
            print(
                f"Legacy batch without record_count: {path.name} "
                f"({len(videos)} records)"
            )

        elif (
            not isinstance(record_count, int)
            or isinstance(record_count, bool)
            or record_count != len(videos)
        ):
            raise ValueError(
                f"Record count mismatch in {path}: "
                f"metadata={record_count}, "
                f"actual={len(videos)}"
            )


        video_ids = []

        for video in videos:
            if not isinstance(video, dict):
                raise ValueError(
                    f"Invalid video record in {path}"
                )

            video_id = video.get("id")

            if not isinstance(video_id, str) or not video_id:
                raise ValueError(
                    f"Invalid video ID in {path}"
                )

            video_ids.append(video_id)

        if len(video_ids) != len(set(video_ids)):
            raise ValueError(
                f"Duplicate video IDs inside {path}"
            )

        location = path.relative_to(
            PROJECT_ROOT
        ).as_posix()

        for video_id in video_ids:
            candidate = (location, operation)
            existing = inventory[channel_id].get(video_id)

            if existing is None:
                inventory[channel_id][video_id] = candidate
                continue

            duplicate_references += 1

            existing_path, existing_operation = existing

            candidate_priority = OPERATION_PRIORITY[operation]
            existing_priority = OPERATION_PRIORITY[
                existing_operation
            ]

            if candidate_priority > existing_priority:
                inventory[channel_id][video_id] = candidate

            elif candidate_priority == existing_priority:
                # Stable choice between equally ranked paths.
                if location < existing_path:
                    inventory[channel_id][video_id] = candidate

        batch_count += 1

    print("Bronze batches scanned:", batch_count)
    print("Batches missing operation metadata:", legacy_count)
    print("Duplicate Bronze references:", duplicate_references)

    return inventory


def prepare_migration_records(registered, inventory):
    """
    Ensure every registered video has a Bronze record.
    Also reject Bronze videos absent from all registries.
    """
    records = []

    all_channels = set(registered) | set(inventory)

    for channel_id in sorted(all_channels):
        registered_ids = registered[channel_id]
        stored_ids = set(inventory[channel_id])

        missing = registered_ids - stored_ids
        unregistered = stored_ids - registered_ids

        if missing:
            raise ValueError(
                f"{len(missing)} missing Bronze videos "
                f"for channel {channel_id}: "
                f"{sorted(missing)[:5]}"
            )

        if unregistered:
            raise ValueError(
                f"{len(unregistered)} unregistered Bronze videos "
                f"for channel {channel_id}: "
                f"{sorted(unregistered)[:5]}"
            )

        for video_id in sorted(registered_ids):
            bronze_path, _ = inventory[channel_id][video_id]

            records.append((
                channel_id,
                video_id,
                bronze_path,
            ))

    return records


def main(apply_changes=False):
    print("========== INGESTED VIDEO MIGRATION ==========")
    print("Mode:", "APPLY" if apply_changes else "DRY RUN")

    # 1. Read checkpoint and daily registry state.
    registered = load_registered_ids()

    # 2. Inspect the physical Bronze files.
    inventory = build_bronze_inventory()

    # 3. Reconcile local state and Bronze inventory.
    records = prepare_migration_records(
        registered,
        inventory,
    )

    if not records:
        raise ValueError(
            "No valid records found for migration"
        )

    print("Validated records:", len(records))

    # 4. Compare the migration inventory with PostgreSQL.
    with get_db_connection() as conn:
        with conn.cursor() as cursor:

            cursor.execute(
                """
                SELECT channel_id
                FROM ingestion.channels
                """
            )

            db_channels = {
                row[0] for row in cursor.fetchall()
            }

            missing_channels = set(registered) - db_channels

            if missing_channels:
                raise ValueError(
                    "Channels missing from PostgreSQL: "
                    f"{sorted(missing_channels)}"
                )

            cursor.execute(
                """
                SELECT channel_id, video_id, bronze_path
                FROM ingestion.ingested_videos
                """
            )

            existing_records = {
                (channel_id, video_id): path
                for channel_id, video_id, path
                in cursor.fetchall()
            }

            new_records = []

            for channel_id, video_id, path in records:
                key = (channel_id, video_id)

                if key in existing_records:
                    if existing_records[key] != path:
                        raise ValueError(
                            f"Conflicting Bronze path for {key}: "
                            f"PostgreSQL={existing_records[key]!r}, "
                            f"Local={path!r}"
                        )
                else:
                    new_records.append(
                        (channel_id, video_id, path)
                    )

            print(
                "Already in PostgreSQL:",
                len(records) - len(new_records),
            )
            print("Records to insert:", len(new_records))

            if not apply_changes:
                print("DRY RUN: No changes made")
                return


            # 5. Perform the migration inside one transaction.

            # Prevent indefinite waits for PostgreSQL locks.
            cursor.execute("SET LOCAL lock_timeout = '5s'")

            print(
                f"Starting migration of {len(new_records)} records...",
                flush=True,
            )

            for index, record in enumerate(new_records, start=1):
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
                    record,
                )

                if index % 100 == 0 or index == len(new_records):
                    print(
                        f"Processed {index}/{len(new_records)} records",
                        flush=True,
                    )

            print(
                "All inserts executed. Committing transaction...",
                flush=True,
            )


    # get_db_connection context commits on successful exit.
    print("Migration completed successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Migrate legacy YouTube ingestion metadata "
            "into PostgreSQL"
        )
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the migration to PostgreSQL",
    )

    args = parser.parse_args()

    main(apply_changes=args.apply)



