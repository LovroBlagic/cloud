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



GCP_PROJECT = os.getenv("PROJECT_ID")
if not GCP_PROJECT:
    raise RuntimeError("GCP_PROJECT environment variable is not set")

DEFAULT_GCS_BUCKET = "reddit-bucket2"
DEFAULT_BQ_TABLE = f"{GCP_PROJECT}.reddit_pipeline.reddit_messages"
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
_storage_client = storage.Client(project=GCP_PROJECT)
_bq_client = bigquery.Client(project=GCP_PROJECT)


def get_config() -> tuple[str, str, str]:
    """
    Returns (bucket_name, bq_table, topic_id).
    Uses env vars if set, otherwise falls back to defaults.
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
    """
    out = dict(obj)

    # id: prefer int if possible
    if "id" in out and out["id"] is not None:
        try:
            out["id"] = int(out["id"])
        except Exception:
            out["id"] = str(out["id"])

    for k in ("title", "author", "subreddit"):
        v = out.get(k, "")
        out[k] = "" if v is None else str(v)

    out["score"] = int(out.get("score", 0))
    out["created_utc"] = int(out["created_utc"])

    out["created_at"] = datetime.fromtimestamp(
        out["created_utc"], tz=timezone.utc
    ).isoformat()

    return out


def gcs_paths(topic_id: str, created_utc: int, message_id: str) -> tuple[str, str]:
    dt = datetime.fromtimestamp(int(created_utc), tz=timezone.utc)
    prefix = (
        f"{topic_id}/"
        f"year={dt.year:04d}/month={dt.month:02d}/"
        f"day={dt.day:02d}/hour={dt.hour:02d}"
    )
    json_path = f"{prefix}/msg-{message_id}.json"
    parquet_path = f"{prefix}/part-{message_id}.parquet"
    return json_path, parquet_path


def upload_json_to_gcs(bucket_name: str, object_path: str, obj: dict) -> None:
    bucket = _storage_client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    blob.upload_from_string(
        json.dumps(obj, ensure_ascii=False),
        content_type="application/json; charset=utf-8",
    )


def upload_parquet_to_gcs(bucket_name: str, object_path: str, obj: dict) -> str:
    df = pd.DataFrame([obj])
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)

    bucket = _storage_client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    blob.upload_from_file(buf, content_type="application/octet-stream")

    return f"gs://{bucket_name}/{object_path}"


def start_load_parquet_to_bigquery(gcs_uri: str, table_id: str) -> str:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
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
    envelope = request.get_json(silent=True) or {}
    msg = envelope.get("message") or {}
    b64 = msg.get("data")

    message_id = msg.get("messageId") or uuid.uuid4().hex
    delivery_attempt = msg.get("deliveryAttempt")

    if not b64:
        logger.warning("No message.data (messageId=%s)", message_id)
        return ("Bad Request: no message.data", 400)

    try:
        bucket_name, bq_table, topic_id = get_config()

        avro_bytes = base64.b64decode(b64)
        obj = decode_avro(avro_bytes)
        obj = normalize_obj(obj)

        logger.info(
            "Decoded messageId=%s deliveryAttempt=%s obj=%s",
            message_id,
            delivery_attempt,
            obj,
        )

        json_path, parquet_path = gcs_paths(
            topic_id, obj["created_utc"], message_id
        )
        upload_json_to_gcs(bucket_name, json_path, obj)
        gcs_uri = upload_parquet_to_gcs(bucket_name, parquet_path, obj)

        job_id = start_load_parquet_to_bigquery(gcs_uri, bq_table)
        logger.info(
            "Started BQ load job_id=%s table=%s uri=%s",
            job_id,
            bq_table,
            gcs_uri,
        )

        return ("", 204)

    except Exception as e:
        logger.exception("Processing failed (messageId=%s): %s", message_id, e)
        return ("Processing failed", 500)


if __name__ == "__main__":
    bucket_name, bq_table, topic_id = get_config()
    logger.info(
        "Starting consumer | project=%s bucket=%s bq_table=%s topic=%s",
        GCP_PROJECT,
        bucket_name,
        bq_table,
        topic_id,
    )
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
