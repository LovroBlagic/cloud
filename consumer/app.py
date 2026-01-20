import base64
import io
import json
import logging
import os
import uuid
from datetime import datetime, timezone

import pandas as pd
from flask import Flask, request
from fastavro import parse_schema, schemaless_reader
from google.cloud import storage, bigquery

# ----------------------------
# Defaults for your lab setup
# ----------------------------
DEFAULT_GCS_BUCKET = "reddit-bucket2"
DEFAULT_BQ_TABLE = "reddit-484216.reddit_pipeline.reddit_messages"
DEFAULT_TOPIC_ID = "reddit-topic"

app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("consumer")

# ---- AVRO schema (must match Pub/Sub Schema Registry) ----
REDDIT_SCHEMA_DICT = {
    "type": "record",
    "name": "RedditPost",
    "namespace": "lab.reddit",
    "fields": [
        {"name": "id", "type": ["int", "string"]},  # matches topic union
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


def get_config() -> tuple[str, str, str]:
    """
    Returns (bucket_name, bq_table, topic_id).
    Uses env vars if set, otherwise falls back to lab defaults.
    """
    bucket_name = os.getenv("GCS_BUCKET", DEFAULT_GCS_BUCKET).strip()
    bq_table = os.getenv("BQ_TABLE", DEFAULT_BQ_TABLE).strip()
    topic_id = os.getenv("TOPIC_ID", DEFAULT_TOPIC_ID).strip()

    if not bucket_name:
        raise RuntimeError("GCS_BUCKET is empty")
    if not bq_table:
        raise RuntimeError("BQ_TABLE is empty")
    if not topic_id:
        raise RuntimeError("TOPIC_ID is empty")

    return bucket_name, bq_table, topic_id


def decode_avro(avro_bytes: bytes) -> dict:
    """Decode AVRO binary bytes into a Python dict."""
    bio = io.BytesIO(avro_bytes)
    return schemaless_reader(bio, PARSED_SCHEMA)


def normalize_obj(obj: dict) -> dict:
    """
    Ensure types are consistent with the AVRO schema and BigQuery-friendly.
    - id is union ["int","string"] -> keep as int where possible, else string
    - created_utc long -> int
    - score int -> int
    - strings -> str (never None because schema says "string")
    """
    out = dict(obj)

    # id: prefer int branch if it looks numeric, else string branch
    if "id" in out and out["id"] is not None:
        try:
            out["id"] = int(out["id"])
        except Exception:
            out["id"] = str(out["id"])

    # required non-null strings in your schema
    for k in ("title", "author", "subreddit"):
        v = out.get(k, "")
        out[k] = "" if v is None else str(v)

    out["score"] = int(out.get("score", 0))
    out["created_utc"] = int(out["created_utc"])

    # Optional: add created_at ISO string
    out["created_at"] = datetime.fromtimestamp(out["created_utc"], tz=timezone.utc).isoformat()

    return out


def gcs_paths(topic_id: str, created_utc: int, message_id: str) -> tuple[str, str]:
    """
    Partitioned GCS object paths (unique per Pub/Sub message_id):
    - JSON:    topic/year=YYYY/month=MM/day=DD/hour=HH/msg-<message_id>.json
    - Parquet: topic/year=YYYY/month=MM/day=DD/hour=HH/part-<message_id>.parquet
    """
    dt = datetime.fromtimestamp(int(created_utc), tz=timezone.utc)
    prefix = (
        f"{topic_id}/"
        f"year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}"
    )
    json_path = f"{prefix}/msg-{message_id}.json"
    parquet_path = f"{prefix}/part-{message_id}.parquet"
    return json_path, parquet_path


def upload_json_to_gcs(bucket_name: str, object_path: str, obj: dict) -> None:
    bucket = _storage_client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    blob.upload_from_string(
        data=json.dumps(obj, ensure_ascii=False),
        content_type="application/json; charset=utf-8",
    )


def upload_parquet_to_gcs(bucket_name: str, object_path: str, obj: dict) -> str:
    """Write a single-message Parquet file to GCS and return the gs:// URI."""
    df = pd.DataFrame([obj])

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)  # requires pyarrow
    buf.seek(0)

    bucket = _storage_client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    blob.upload_from_file(buf, content_type="application/octet-stream")

    return f"gs://{bucket_name}/{object_path}"


def start_load_parquet_to_bigquery(gcs_uri: str, table_id: str) -> str:
    """
    Start a BigQuery load job and return job_id.
    IMPORTANT: do NOT wait (.result()) inside a Pub/Sub push request.
    """
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # Removed schema_update_options to avoid "too many table update operations"
    )
    load_job = _bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    return load_job.job_id


@app.get("/listening")
def listening_check():
    bucket_name, bq_table, topic_id = get_config()
    return (
        f"OK - listening | bucket={bucket_name} | bq_table={bq_table} | topic={topic_id}",
        200,
    )


@app.post("/push")
def pubsub_push():
    """
    Pub/Sub push format:
    {
      "message": {"data": "base64-encoded-bytes", "messageId": "...", ...},
      "subscription": "..."
    }

    Return codes:
    - 204 = ack
    - 4xx = drop (do not retry forever for bad requests)
    - 5xx = nack (retry)
    """
    envelope = request.get_json(silent=True) or {}
    msg = envelope.get("message") or {}
    b64 = msg.get("data")

    # helpful for debugging retries / duplicates
    message_id = msg.get("messageId") or uuid.uuid4().hex
    delivery_attempt = msg.get("deliveryAttempt")

    if not b64:
        logger.warning("No message.data (messageId=%s): %s", message_id, envelope)
        return ("Bad Request: no message.data", 400)

    try:
        bucket_name, bq_table, topic_id = get_config()

        # 1) base64 -> AVRO bytes
        avro_bytes = base64.b64decode(b64)

        # 2) AVRO -> dict
        obj = decode_avro(avro_bytes)

        # 3) normalize types / add created_at
        obj = normalize_obj(obj)

        logger.info(
            "Decoded messageId=%s deliveryAttempt=%s obj=%s",
            message_id,
            delivery_attempt,
            obj,
        )

        # 4) Store to GCS (use message_id to avoid overwrites)
        json_path, parquet_path = gcs_paths(topic_id, obj["created_utc"], message_id)
        upload_json_to_gcs(bucket_name, json_path, obj)
        gcs_uri = upload_parquet_to_gcs(bucket_name, parquet_path, obj)

        logger.info(
            "Stored to GCS messageId=%s json=gs://%s/%s parquet=%s",
            message_id,
            bucket_name,
            json_path,
            gcs_uri,
        )

        # 5) Start BigQuery load (do not block)
        job_id = start_load_parquet_to_bigquery(gcs_uri, bq_table)
        logger.info(
            "Started BQ load job_id=%s table=%s uri=%s (messageId=%s)",
            job_id,
            bq_table,
            gcs_uri,
            message_id,
        )

        # ACK immediately so Pub/Sub doesn't retry storm on long-running loads
        return ("", 204)

    except Exception as e:
        # Returning 500 tells Pub/Sub to retry; keep this only for truly transient errors.
        logger.exception("Processing failed (messageId=%s): %s", message_id, e)
        return ("Processing failed", 500)


if __name__ == "__main__":
    try:
        bucket_name, bq_table, topic_id = get_config()
        logger.info(
            "Starting consumer with config: bucket=%s bq_table=%s topic=%s",
            bucket_name,
            bq_table,
            topic_id,
        )
    except Exception as e:
        logger.error("Startup config error: %s", e)

    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
