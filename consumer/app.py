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

# ----------------------------
# Defaults for your lab setup
# ----------------------------
DEFAULT_GCS_BUCKET = "reddit-bucket2"
DEFAULT_BQ_TABLE = "reddit-484216.reddit_pipeline.reddit_messages"
DEFAULT_TOPIC_ID = "reddit-topic"

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



def gcs_paths(topic_id: str, created_utc: int) -> tuple[str, str]:
    """
    Partitioned GCS object paths:
    - JSON:    topic/year=YYYY/month=MM/day=DD/hour=HH/msg-<ts>.json
    - Parquet: topic/year=YYYY/month=MM/day=DD/hour=HH/part-<ts>.parquet
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
    """Write a single-message Parquet file to GCS and return the gs:// URI."""
    df = pd.DataFrame([obj])

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)  # requires pyarrow (recommended)
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
    # Helpful to confirm what config the service is using
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
      "message": {"data": "base64-encoded-bytes", ...},
      "subscription": "..."
    }

    Return codes:
    - 204 = ack
    - 5xx = nack (retry -> after max attempts goes to dead-letter topic)
    """
    envelope = request.get_json(silent=True) or {}
    msg = envelope.get("message") or {}
    b64 = msg.get("data")

    if not b64:
        logger.warning("No message.data in request: %s", envelope)
        return ("Bad Request: no message.data", 400)

    try:
        bucket_name, bq_table, topic_id = get_config()

        # 1) base64 -> AVRO bytes
        avro_bytes = base64.b64decode(b64)

        # 2) AVRO -> dict
        obj = decode_avro(avro_bytes)

        if obj.get("id") == 999:
            raise RuntimeError("Intentional failure to trigger dead-letter")


        # Optional: add created_at TIMESTAMP (recommended for BigQuery analytics)
        obj["created_at"] = datetime.fromtimestamp(
            int(obj["created_utc"]), tz=timezone.utc
        ).isoformat()

        logger.info("Decoded AVRO message: %s", obj)

        # 3) Store to GCS
        json_path, parquet_path = gcs_paths(topic_id, obj["created_utc"])
        upload_json_to_gcs(bucket_name, json_path, obj)
        gcs_uri = upload_parquet_to_gcs(bucket_name, parquet_path, obj)
        logger.info(
            "Stored JSON+Parquet to GCS: gs://%s/%s and %s",
            bucket_name,
            json_path,
            gcs_uri,
        )

        # 4) Load Parquet -> BigQuery
        load_parquet_to_bigquery(gcs_uri, bq_table)
        logger.info("Loaded into BigQuery: %s from %s", bq_table, gcs_uri)

        return ("", 204)

    except Exception as e:
        logger.exception("Processing failed (will retry / dead-letter): %s", e)
        return ("Processing failed", 500)


if __name__ == "__main__":
    # Log the config once at startup
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
