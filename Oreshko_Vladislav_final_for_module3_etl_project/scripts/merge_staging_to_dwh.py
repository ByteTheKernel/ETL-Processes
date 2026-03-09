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

SQL_FILE_MAP = {
    "user_sessions": "/opt/airflow/sql/06_merge_user_sessions.sql",
    "event_logs": "/opt/airflow/sql/07_merge_event_logs.sql",
    "support_tickets": "/opt/airflow/sql/08_merge_support_tickets.sql",
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


def finish_job_log(
    pg_conn,
    log_id: int,
    rows_merged_dwh: int,
    status: str,
    error_message: str | None = None,
) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            update meta.etl_job_log
            set rows_merged_dwh = %s,
                status = %s,
                error_message = %s,
                finished_at = current_timestamp
            where id = %s
            """,
            (rows_merged_dwh, status, error_message, log_id),
        )
    pg_conn.commit()


def get_dwh_count(pg_conn, source_name: str) -> int:
    table_map = {
        "user_sessions": "dwh.user_sessions",
        "event_logs": "dwh.event_logs",
        "support_tickets": "dwh.support_tickets",
    }
    table_name = table_map[source_name]

    with pg_conn.cursor() as cur:
        cur.execute(f"select count(*) from {table_name}")
        return cur.fetchone()[0]


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in SQL_FILE_MAP:
        print("Usage: python merge_staging_to_dwh.py [user_sessions|event_logs|support_tickets]")
        sys.exit(1)

    source_name = sys.argv[1]
    sql_file_path = SQL_FILE_MAP[source_name]
    job_name = "merge_staging_to_dwh"
    run_id = str(uuid.uuid4())
    batch_id = f"{source_name}_merge_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}"

    pg_conn = None
    log_id = None

    try:
        pg_conn = get_pg_connection()
        log_id = insert_job_log_start(pg_conn, job_name, source_name, run_id, batch_id)

        before_count = get_dwh_count(pg_conn, source_name)

        with open(sql_file_path, "r", encoding="utf-8") as f:
            sql_text = f.read()

        with pg_conn.cursor() as cur:
            cur.execute(sql_text)
        pg_conn.commit()

        after_count = get_dwh_count(pg_conn, source_name)
        merged_rows = after_count - before_count if after_count >= before_count else after_count

        finish_job_log(
            pg_conn=pg_conn,
            log_id=log_id,
            rows_merged_dwh=merged_rows,
            status="success",
        )

        print(f"Source: {source_name}")
        print(f"Rows merged into DWH: {merged_rows}")
        print(f"DWH count before: {before_count}")
        print(f"DWH count after: {after_count}")

    except Exception as exc:
        if pg_conn is not None and log_id is not None:
            finish_job_log(
                pg_conn=pg_conn,
                log_id=log_id,
                rows_merged_dwh=0,
                status="failed",
                error_message=str(exc),
            )
        raise

    finally:
        if pg_conn is not None:
            pg_conn.close()


if __name__ == "__main__":
    main()