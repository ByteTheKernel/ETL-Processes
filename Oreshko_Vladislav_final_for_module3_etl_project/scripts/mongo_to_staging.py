#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import sys
import uuid
from datetime import datetime, UTC
from typing import Any, Optional

import psycopg2
from psycopg2.extras import execute_batch, Json
from pymongo import MongoClient


MONGO_HOST = os.getenv("MONGO_HOST", "mongo")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME", "mongo_admin")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "mongo_password")
MONGO_DB = os.getenv("MONGO_INITDB_DATABASE", "etl_mongo")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "etl_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "etl_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "etl_password")


SOURCE_CONFIG = {
    "user_sessions": {
        "mongo_collection": "user_sessions",
        "watermark_field": "start_time",
        "staging_table": "staging.user_sessions_raw",
        "business_key": "session_id",
        "staging_watermark_column": "source_start_time",
    },
    "event_logs": {
        "mongo_collection": "event_logs",
        "watermark_field": "timestamp",
        "staging_table": "staging.event_logs_raw",
        "business_key": "event_id",
        "staging_watermark_column": "source_timestamp",
    },
    "support_tickets": {
        "mongo_collection": "support_tickets",
        "watermark_field": "updated_at",
        "staging_table": "staging.support_tickets_raw",
        "business_key": "ticket_id",
        "staging_watermark_column": "source_updated_at",
    },
}


def get_pg_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def get_mongo_client():
    return MongoClient(
        host=MONGO_HOST,
        port=MONGO_PORT,
        username=MONGO_USER,
        password=MONGO_PASSWORD,
    )


def convert_for_json(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [convert_for_json(v) for v in value]
    if isinstance(value, dict):
        return {k: convert_for_json(v) for k, v in value.items()}
    return value


def normalize_mongo_doc(doc: dict) -> dict:
    normalized = {}
    for key, value in doc.items():
        if key == "_id":
            normalized[key] = str(value)
        else:
            normalized[key] = convert_for_json(value)
    return normalized


def get_last_watermark(pg_conn, source_name: str) -> Optional[datetime]:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            select last_watermark_value
            from meta.etl_watermarks
            where source_name = %s
            """,
            (source_name,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def insert_job_log_start(pg_conn, job_name: str, source_name: str, run_id: str, batch_id: str) -> int:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            insert into meta.etl_job_log (
                job_name,
                source_name,
                run_id,
                batch_id,
                status,
                started_at
            )
            values (%s, %s, %s, %s, %s, current_timestamp)
            returning id
            """,
            (job_name, source_name, run_id, batch_id, "running"),
        )
        log_id = cur.fetchone()[0]
    pg_conn.commit()
    return log_id


def finish_job_log(
    pg_conn,
    log_id: int,
    rows_extracted: int,
    rows_loaded_staging: int,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            update meta.etl_job_log
            set rows_extracted = %s,
                rows_loaded_staging = %s,
                status = %s,
                error_message = %s,
                finished_at = current_timestamp
            where id = %s
            """,
            (rows_extracted, rows_loaded_staging, status, error_message, log_id),
        )
    pg_conn.commit()


def extract_documents(mongo_db, source_name: str, last_watermark: Optional[datetime]) -> list[dict]:
    cfg = SOURCE_CONFIG[source_name]
    collection = mongo_db[cfg["mongo_collection"]]
    watermark_field = cfg["watermark_field"]

    query = {}
    if last_watermark is not None:
        query[watermark_field] = {"$gt": last_watermark}

    docs = list(collection.find(query).sort(watermark_field, 1))
    return docs


def load_to_staging(pg_conn, source_name: str, docs: list[dict], batch_id: str) -> int:
    if not docs:
        return 0

    cfg = SOURCE_CONFIG[source_name]
    business_key = cfg["business_key"]
    watermark_field = cfg["watermark_field"]
    staging_table = cfg["staging_table"]
    staging_watermark_column = cfg["staging_watermark_column"]

    rows = []
    for doc in docs:
        normalized_doc = normalize_mongo_doc(doc)
        rows.append(
            (
                doc[business_key],
                Json(normalized_doc),
                doc[watermark_field],
                batch_id,
            )
        )

    insert_sql = f"""
        insert into {staging_table} (
            {business_key},
            source_doc,
            {staging_watermark_column},
            batch_id
        )
        values (%s, %s, %s, %s)
    """

    with pg_conn.cursor() as cur:
        execute_batch(cur, insert_sql, rows, page_size=500)

    pg_conn.commit()
    return len(rows)


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in SOURCE_CONFIG:
        print("Usage: python mongo_to_staging.py [user_sessions|event_logs|support_tickets]")
        sys.exit(1)

    source_name = sys.argv[1]
    job_name = "mongo_to_staging"
    run_id = str(uuid.uuid4())
    batch_id = f"{source_name}_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}"

    pg_conn = None
    mongo_client = None
    log_id = None

    try:
        pg_conn = get_pg_connection()
        mongo_client = get_mongo_client()
        mongo_db = mongo_client[MONGO_DB]

        log_id = insert_job_log_start(pg_conn, job_name, source_name, run_id, batch_id)

        last_watermark = get_last_watermark(pg_conn, source_name)
        docs = extract_documents(mongo_db, source_name, last_watermark)
        rows_extracted = len(docs)

        rows_loaded_staging = load_to_staging(pg_conn, source_name, docs, batch_id)

        finish_job_log(
            pg_conn=pg_conn,
            log_id=log_id,
            rows_extracted=rows_extracted,
            rows_loaded_staging=rows_loaded_staging,
            status="success",
        )

        print(f"Source: {source_name}")
        print(f"Last watermark: {last_watermark}")
        print(f"Extracted: {rows_extracted}")
        print(f"Loaded to staging: {rows_loaded_staging}")
        print(f"Batch ID: {batch_id}")

    except Exception as exc:
        if pg_conn is not None and log_id is not None:
            finish_job_log(
                pg_conn=pg_conn,
                log_id=log_id,
                rows_extracted=0,
                rows_loaded_staging=0,
                status="failed",
                error_message=str(exc),
            )
        raise

    finally:
        if mongo_client is not None:
            mongo_client.close()
        if pg_conn is not None:
            pg_conn.close()


if __name__ == "__main__":
    main()