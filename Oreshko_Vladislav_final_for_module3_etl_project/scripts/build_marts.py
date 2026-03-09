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
    "user_activity": "/opt/airflow/sql/10_build_mart_user_activity.sql",
    "support_efficiency": "/opt/airflow/sql/11_build_mart_support_efficiency.sql",
}


def get_pg_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def insert_job_log_start(pg_conn, mart_name: str, run_id: str, batch_id: str) -> int:
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
            ("build_mart", mart_name, run_id, batch_id, "running"),
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


def get_mart_count(pg_conn, mart_name: str) -> int:
    table_map = {
        "user_activity": "mart.user_activity",
        "support_efficiency": "mart.support_efficiency",
    }
    table_name = table_map[mart_name]

    with pg_conn.cursor() as cur:
        cur.execute(f"select count(*) from {table_name}")
        return cur.fetchone()[0]


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in SQL_FILE_MAP:
        print("Usage: python build_marts.py [user_activity|support_efficiency]")
        sys.exit(1)

    mart_name = sys.argv[1]
    sql_file_path = SQL_FILE_MAP[mart_name]
    run_id = str(uuid.uuid4())
    batch_id = f"{mart_name}_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}"

    pg_conn = None
    log_id = None

    try:
        pg_conn = get_pg_connection()
        log_id = insert_job_log_start(pg_conn, mart_name, run_id, batch_id)

        with open(sql_file_path, "r", encoding="utf-8") as f:
            sql_text = f.read()

        with pg_conn.cursor() as cur:
            cur.execute(sql_text)
        pg_conn.commit()

        mart_count = get_mart_count(pg_conn, mart_name)

        finish_job_log(pg_conn, log_id, status="success")

        print(f"Mart: {mart_name}")
        print(f"Rows in mart: {mart_count}")

    except Exception as exc:
        if pg_conn is not None and log_id is not None:
            finish_job_log(pg_conn, log_id, status="failed", error_message=str(exc))
        raise

    finally:
        if pg_conn is not None:
            pg_conn.close()


if __name__ == "__main__":
    main()