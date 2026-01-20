import base64
import io
import json
import logging
import os
from datetime import datetime, timezone

import pandas as pd
from flask import Flask, request
from fastavro import parse_schema, schemaless_reader
from google.cloud import storage, bigquery

app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("consumer")

# ---- AVRO schema (mora biti identična onoj u Pub/Sub Schema Registry) ----
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

# ---- Clients (re-use across requests) ----
_storage_client = storage.Client()
_bq_client = bigquery.Client()


def decode_avro(avro_bytes: bytes) -> dict:
    """Decode AVRO binary bytes into a Python dict."""
    bio = io.BytesIO(avro_bytes)
    return schemaless_reader(bio, PARSED_SCHEMA)


def gcs_paths(bucket_name: str, topic_id: str, created_utc: int) -> tuple[str, str]:
    """
    Build partitioned GCS object paths:
    - JSON:   topic/year=YYYY/month=MM/day=DD/hour=HH/<id>.json
    - Parquet:topic/year=YYYY/month=MM/day=DD/hour=HH/part-<timestamp>.parquet
    """
    dt = datetime.fromtimestamp(int(created_utc), tz=timezone.utc)
    prefix = (
        f"{topic_id}/"
        f"year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}"
    )

    json_path = f"{prefix}/msg-{int(dt.timestamp())}.json"
    parquet_path = f"{prefix}/part-{int(dt.timestamp())}.parquet"
    return json_path, parquet_path


def upload_json_to_gcs(bucket_name: str, object_path: str, obj: dict) -> None:
    bucket = _storage_client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    blob.upload_from_string(
        data=json.dumps(obj, ensure_ascii=False),
        content_type="application/json; charset=utf-8",
    )


def upload_parquet_to_gcs(bucket_name: str, object_path: str, obj: dict) -> str:
    """
    Write a single-message Parquet file to GCS.
    Returns the gs:// URI (needed for BigQuery load).
    """
    df = pd.DataFrame([obj])

    buf = io.BytesIO()
    # requires pyarrow or fastparquet installed in the image
    df.to_parquet(buf, index=False)
    buf.seek(0)

    bucket = _storage_client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    blob.upload_from_file(buf, content_type="application/octet-stream")

    return f"gs://{bucket_name}/{object_path}"


def load_parquet_to_bigquery(gcs_uri: str, table_id: str) -> None:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
        ],
    )
    load_job = _bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()


@app.get("/listening")
def listening_check():
    return "Consumer service is listening", 200


@app.post("/push")
def pubsub_push():
    """
    Pub/Sub push format:
    {
      "message": {
        "data": "base64-encoded-bytes",
        "messageId": "...",
        ...
      },
      "subscription": "..."
    }

    Return codes:
    - 204 = ack
    - 5xx = nack (Pub/Sub will retry; after max attempts -> dead-letter topic)
    """
    envelope = request.get_json(silent=True) or {}
    msg = envelope.get("message") or {}
    b64 = msg.get("data")

    if not b64:
        logger.warning("No message.data in request: %s", envelope)
        # treat as bad request (won't help to retry)
        return ("Bad Request: no message.data", 400)

    # --- Config (env vars) ---
    bucket_name = os.getenv("GCS_BUCKET")
    topic_id = os.getenv("TOPIC_ID", "reddit-topic")
    bq_table = os.getenv("BQ_TABLE")  # e.g. "my-project:reddit_pipeline.reddit_messages" OR "my-project.reddit_pipeline.reddit_messages"

    if not bucket_name or not bq_table:
        logger.error("Missing env vars: GCS_BUCKET and/or BQ_TABLE")
        # retry won't fix misconfig; but failing fast makes it obvious
        return ("Server misconfigured", 500)

    try:
        # 1) Decode base64 -> AVRO bytes
        avro_bytes = base64.b64decode(b64)

        # 2) Decode AVRO -> dict
        obj = decode_avro(avro_bytes)
        logger.info("Decoded AVRO message: %s", obj)

        # 3) Store to GCS (JSON + Parquet, partitioned)
        json_path, parquet_path = gcs_paths(bucket_name, topic_id, obj["created_utc"])
        upload_json_to_gcs(bucket_name, json_path, obj)
        gcs_uri = upload_parquet_to_gcs(bucket_name, parquet_path, obj)
        logger.info("Stored JSON+Parquet to GCS: gs://%s/%s and %s", bucket_name, json_path, gcs_uri)

        # 4) Load Parquet -> BigQuery (append)
        load_parquet_to_bigquery(gcs_uri, bq_table)
        logger.info("Loaded into BigQuery: %s from %s", bq_table, gcs_uri)

        # ACK
        return ("", 204)

    except Exception as e:
        # Important: 5xx makes Pub/Sub retry; after max delivery attempts -> dead-letter topic
        logger.exception("Processing failed, will trigger retry/dead-letter: %s", e)
        return ("Processing failed", 500)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
