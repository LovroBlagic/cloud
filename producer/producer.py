import json
import os
import time
import io
from pathlib import Path

from google.cloud import pubsub_v1
from fastavro import parse_schema, schemaless_writer


# AVRO schema (must match Pub/Sub schema)
REDDIT_SCHEMA_DICT = {
    "type": "record",
    "name": "RedditPost",
    "namespace": "lab.reddit",
    "fields": [
        {"name": "id", "type": ["int", "string"]},
        {"name": "title", "type": "string"},
        {"name": "author", "type": "string"},
        {"name": "score", "type": "int"},
        {"name": "subreddit", "type": "string"},
        {"name": "created_utc", "type": "long"},
    ],
}

PARSED_SCHEMA = parse_schema(REDDIT_SCHEMA_DICT)


def encode_avro(record: dict) -> bytes:
    bio = io.BytesIO()
    schemaless_writer(bio, PARSED_SCHEMA, record)
    return bio.getvalue()


def main():
    project_id = os.getenv("PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
    topic_id = os.getenv("TOPIC_ID")
    data_path = Path(os.getenv("DATA_PATH", "data.json"))

    delay = 3  # fixed delay as required

    if not project_id or not topic_id:
        raise RuntimeError("Set PROJECT_ID (or GOOGLE_CLOUD_PROJECT) and TOPIC_ID")

    items = json.loads(data_path.read_text(encoding="utf-8"))
    if not isinstance(items, list):
        raise ValueError("data.json must contain a JSON array")

    # 🚨 DLQ TEST MESSAGE (VALID AVRO, consumer will fail on id=999)
    dlq_test_item = {
        "id": 999,
        "title": "FORCE_DEAD_LETTER",
        "author": "tester",
        "score": 0,
        "subreddit": "dataengineering",
        "created_utc": 1737078000,
    }

    items.append(dlq_test_item)

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    for i, item in enumerate(items, start=1):
        try:
            payload = encode_avro(item)
            msg_id = publisher.publish(topic_path, payload).result(timeout=30)
            print(f"published {i}/{len(items)} msg_id={msg_id} id={item.get('id')}")
        except Exception as e:
            print(f"FAILED {i}/{len(items)} id={item.get('id')} error={e}")

        time.sleep(delay)


if __name__ == "__main__":
    main()
