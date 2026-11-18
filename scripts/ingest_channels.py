
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://www.googleapis.com/youtube/v3"
OUTPUT_DIR = Path("data/bronze/youtube/channels")


def get_session():
    """Create an HTTP session with retry support."""
    session = requests.Session()

    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)

    return session


def extract_channel(session, api_key, handle):
    """Retrieve metadata for one YouTube channel."""
    response = session.get(
        f"{BASE_URL}/channels",
        params={
            "part": "snippet,statistics,contentDetails",
            "forHandle": handle,
            "key": api_key,
        },
        timeout=30,
    )

    response.raise_for_status()
    payload = response.json()

    if not payload.get("items"):
        raise ValueError(f"No channel found: {handle}")

    return payload


def save_raw(payload, handle, category, run_id):
    """Persist an ingestion envelope as JSON."""
    ingested_at = datetime.now(timezone.utc)
    date_partition = ingested_at.strftime("%Y-%m-%d")

    output_path = OUTPUT_DIR / f"ingestion_date={date_partition}"
    output_path.mkdir(parents=True, exist_ok=True)

    channel = payload["items"][0]
    channel_id = channel["id"]

    document = {
        "metadata": {
            "source": "youtube_data_api_v3",
            "entity": "channel",
            "channel_id": channel_id,
            "handle": handle,
            "category": category,
            "run_id": run_id,
            "ingested_at": ingested_at.isoformat(),
        },
        "raw": payload,
    }

    filename = f"{channel_id}_{run_id}.json"
    destination = output_path / filename
    temporary = destination.with_suffix(".tmp")

    try:
        with temporary.open("w", encoding="utf-8") as file:
            json.dump(document, file, ensure_ascii=False, indent=2)

        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)

    return destination


def main():
    load_dotenv()
    api_key = os.getenv("YOUTUBE_API_KEY")

    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY is missing")

    with open("config/channels.yml", encoding="utf-8") as file:
        registry = yaml.safe_load(file)

    channels = registry["channels"]
    run_id = uuid.uuid4().hex
    failures = []

    with get_session() as session:
        for item in channels:
            handle = item["handle"]
            category = item["category"]

            try:
                payload = extract_channel(
                    session, api_key, handle
                )

                path = save_raw(
                    payload, handle, category, run_id
                )

                channel = payload["items"][0]
                title = channel["snippet"]["title"]

                print(f"[SUCCESS] {title} -> {path}")

            except (requests.RequestException, ValueError, KeyError) as exc:
                print(f"[FAILED] {handle}: {exc}")
                failures.append(handle)

    if failures:
        print(f"Failed channels: {', '.join(failures)}")
        sys.exit(1)

    print("All channel ingestions completed successfully.")


if __name__ == "__main__":
    main()
