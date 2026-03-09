#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import uuid
from datetime import datetime, UTC

import psycopg2


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "etl_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "etl_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "etl_password")

SOURCE_CONFIG = {
    "user_sessions": {
        "staging_table": "staging.user_sessions_raw",
        "staging_watermark_column": "source_start_time",
    },
    "event_logs": {
        "staging_table": "staging.event_logs_raw",
        "staging_watermark_column": "source_timestamp",
    },
    "support_tickets": {
        "staging_table": "staging.support_tickets_raw",
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


def finish_job_log(pg_conn, log_id: int, status: str, error_message: str | None = None) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            update meta.etl_job_log
            set status = %s,
                error_message = %s,
                finished_at = current_timestamp
            where id = %s
            """,
            (status, error_message, log_id),
        )
    pg_conn.commit()


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in SOURCE_CONFIG:
        print("Usage: python update_watermark.py [user_sessions|event_logs|support_tickets]")
        sys.exit(1)

    source_name = sys.argv[1]
    cfg = SOURCE_CONFIG[source_name]
    run_id = str(uuid.uuid4())
    batch_id = f"{source_name}_watermark_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}"

    pg_conn = None
    log_id = None

    try:
        pg_conn = get_pg_connection()
        log_id = insert_job_log_start(pg_conn, "update_watermark", source_name, run_id, batch_id)

        with pg_conn.cursor() as cur:
            cur.execute(
                f"""
                select max({cfg['staging_watermark_column']})
                from {cfg['staging_table']}
                """
            )
            max_watermark = cur.fetchone()[0]

        if max_watermark is None:
            finish_job_log(pg_conn, log_id, status="success")
            print(f"No data found in staging for {source_name}. Watermark was not changed.")
            return

        with pg_conn.cursor() as cur:
            cur.execute(
                """
                update meta.etl_watermarks
                set last_watermark_value = %s,
                    last_run_at = current_timestamp,
                    last_status = %s
                where source_name = %s
                """,
                (max_watermark, "success", source_name),
            )
        pg_conn.commit()

        finish_job_log(pg_conn, log_id, status="success")
        print(f"Updated watermark for {source_name}: {max_watermark}")

    finally:
        if pg_conn is not None:
            pg_conn.close()


if __name__ == "__main__":
    main()