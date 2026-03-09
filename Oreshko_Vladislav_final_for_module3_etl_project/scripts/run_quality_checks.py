#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import uuid
from datetime import datetime, UTC

import psycopg2


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "etl_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "etl_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "etl_password")

SQL_FILE_PATH = "/opt/airflow/sql/09_quality_checks.sql"


def get_pg_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def insert_job_log_start(pg_conn, job_name: str, run_id: str, batch_id: str) -> int:
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
            (job_name, "all_sources", run_id, batch_id, "running"),
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
    pg_conn = None
    log_id = None
    run_id = str(uuid.uuid4())
    batch_id = f"quality_checks_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}"

    try:
        pg_conn = get_pg_connection()
        log_id = insert_job_log_start(pg_conn, "run_quality_checks", run_id, batch_id)

        with open(SQL_FILE_PATH, "r", encoding="utf-8") as f:
            sql_text = f.read()

        with pg_conn.cursor() as cur:
            cur.execute(sql_text)
            rows = cur.fetchall()

        total_errors = 0
        print("Quality check results:")
        for check_name, error_count in rows:
            print(f"{check_name}: {error_count}")
            total_errors += error_count

        if total_errors > 0:
            finish_job_log(
                pg_conn,
                log_id,
                status="failed",
                error_message=f"Quality checks failed. Total errors: {total_errors}",
            )
            raise RuntimeError(f"Quality checks failed. Total errors: {total_errors}")

        finish_job_log(pg_conn, log_id, status="success")
        print("All quality checks passed successfully.")

    finally:
        if pg_conn is not None:
            pg_conn.close()


if __name__ == "__main__":
    main()