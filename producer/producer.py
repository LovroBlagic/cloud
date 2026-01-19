import json
import os
import time
from pathlib import Path
from google.cloud import pubsub_v1


def main():
    project_id = os.getenv("PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
    topic_id = os.getenv("TOPIC_ID")
    data_path = Path(os.getenv("DATA_PATH", "data.json"))
    delay = float(os.getenv("DELAY_SECONDS", "0"))

    if not project_id or not topic_id:
        raise RuntimeError("Set PROJECT_ID (or GOOGLE_CLOUD_PROJECT) and TOPIC_ID")

    items = json.loads(data_path.read_text(encoding="utf-8"))
    if not isinstance(items, list):
        raise ValueError("data.json must contain a JSON array")

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    for i, item in enumerate(items, start=1):
        payload = json.dumps(item, ensure_ascii=False).encode("utf-8")
        msg_id = publisher.publish(topic_path, payload).result(timeout=30)
        print(f"published {i}/{len(items)} msg_id={msg_id} id={item.get('id')}")
        if delay:
            time.sleep(delay)


if __name__ == "__main__":
    main()
