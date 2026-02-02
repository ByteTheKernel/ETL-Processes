from __future__ import annotations

import os
import re
import csv
from pathlib import Path
from typing import Dict, List, Tuple

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime


DATA_DIR = Path("/data")

# --- helpers ---------------------------------------------------------------

def _sanitize_col(name: str) -> str:
    """
    Приводим имя колонки к postgres-friendly:
    - lower
    - заменяем всё кроме [a-z0-9_] на _
    - сжимаем повторяющиеся _
    - если начинается не с буквы/_, добавляем префикс col_
    """
    n = name.strip().lower()
    n = re.sub(r"[^a-z0-9_]+", "_", n)
    n = re.sub(r"_+", "_", n).strip("_")
    if not n:
        n = "col_unnamed"
    if not re.match(r"^[a-z_]", n):
        n = f"col_{n}"
    return n


def _pick_first_csv(data_dir: Path) -> Path:
    csvs = sorted(data_dir.glob("*.csv"))
    if not csvs:
        raise FileNotFoundError(
            f"В {data_dir} не найдено ни одного .csv. "
            f"Положи распакованный CSV датасета в ./data"
        )
    return csvs[0]


def _read_header(csv_path: Path) -> List[str]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
    if not header:
        raise ValueError(f"Пустой header в файле: {csv_path}")
    return header


def _infer_required_columns(sanitized: List[str]) -> Tuple[str, str, str]:
    """
    Пытаемся подобрать:
    - temperature column
    - noted_date column
    - out/in column
    """
    # temperature
    temp_candidates = [
        "temp", "temperature", "temp_c", "tempf", "temperature_c", "temperaturef"
    ]
    # noted_date
    date_candidates = [
        "noted_date", "noteddate", "noted_dt", "date", "datetime", "timestamp", "time"
    ]
    # out/in
    outin_candidates = [
        "out_in", "out_in_", "outin", "out_in_flag", "in_out", "out_in_status"
    ]

    def find_any(cands: List[str]) -> str | None:
        for c in cands:
            if c in sanitized:
                return c
        return None

    temp_col = find_any(temp_candidates)
    date_col = find_any(date_candidates)

    # out/in иногда бывает "out/in" -> sanitize => "out_in"
    outin_col = None
    for c in ["out_in", "out_in_"] + outin_candidates:
        if c in sanitized:
            outin_col = c
            break

    if not temp_col:
        raise ValueError(
            f"Не удалось определить колонку температуры. "
            f"Найденные колонки: {sanitized}"
        )
    if not date_col:
        raise ValueError(
            f"Не удалось определить колонку даты (noted_date). "
            f"Найденные колонки: {sanitized}"
        )
    if not outin_col:
        raise ValueError(
            f"Не удалось определить колонку out/in. "
            f"Ожидалось что-то вроде out/in -> out_in. "
            f"Найденные колонки: {sanitized}"
        )

    return temp_col, date_col, outin_col


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# --- DAG -------------------------------------------------------------------

with DAG(
    dag_id="task2_transform_temperature",
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
    tags=["etl", "hw2", "transform"],
) as dag:

    @task
    def find_dataset_csv() -> str:
        csv_path = _pick_first_csv(DATA_DIR)
        return str(csv_path)

    @task
    def prepare_tables(csv_path: str) -> Dict[str, str]:
        """
        1) читаем header
        2) создаём staging таблицу под sanitized-колонки (всё TEXT)
        3) создаём таблицы результатов (clean + extremes)
        4) определяем нужные колонки (temp, noted_date, out/in)
        """
        p = Path(csv_path)
        header = _read_header(p)
        sanitized = [_sanitize_col(h) for h in header]

        temp_col, date_col, outin_col = _infer_required_columns(sanitized)

        # DDL
        cols_ddl = ",\n  ".join(f"{_quote_ident(c)} text" for c in sanitized)

        staging_table = "etl.stg_iot_temperature"
        clean_table = "etl.dwh_iot_temperature_clean"
        extremes_table = "etl.mart_iot_temperature_extremes"

        hook = PostgresHook(postgres_conn_id="warehouse")
        with hook.get_conn() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS etl;")
                cur.execute(f"DROP TABLE IF EXISTS {staging_table};")
                cur.execute(f"""
                    CREATE TABLE {staging_table} (
                      {cols_ddl}
                    );
                """)

                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {clean_table} (
                      device_id text,
                      room_id text,
                      location text,
                      outin text,
                      noted_date date,
                      temp_c double precision
                    );
                """)

                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {extremes_table} (
                      year int,
                      noted_date date,
                      avg_temp_c double precision,
                      kind text,         -- 'hottest' / 'coldest'
                      rank_in_kind int
                    );
                """)

        return {
            "staging_table": staging_table,
            "clean_table": clean_table,
            "extremes_table": extremes_table,
            "temp_col": temp_col,
            "date_col": date_col,
            "outin_col": outin_col,
            "sanitized_header": ",".join(sanitized),
        }

    @task
    def load_csv_to_staging(csv_path: str, meta: Dict[str, str]) -> None:
        """
        Грузим CSV в staging через client-side COPY:
        - читаем исходный CSV
        - копируем в staging в порядке sanitized_header
        """
        p = Path(csv_path)
        staging_table = meta["staging_table"]
        sanitized = meta["sanitized_header"].split(",")

        # Сопоставляем исходный header -> sanitized header
        original_header = _read_header(p)
        original_sanitized = [_sanitize_col(h) for h in original_header]

        # Проверяем, что порядок совпадает
        if original_sanitized != sanitized:
            raise ValueError(
                "Несовпадение header. "
                f"original_sanitized={original_sanitized} "
                f"sanitized={sanitized}"
            )

        hook = PostgresHook(postgres_conn_id="warehouse")
        with hook.get_conn() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cols_sql = ", ".join(_quote_ident(c) for c in sanitized)
                copy_sql = f"COPY {staging_table} ({cols_sql}) FROM STDIN WITH (FORMAT csv, HEADER true)"
                with p.open("r", encoding="utf-8") as f:
                    cur.copy_expert(copy_sql, f)

    @task
    def transform_and_mart(meta: Dict[str, str]) -> None:
        """
        1) clean_table:
           - фильтр out/in = 'in'
           - noted_date -> date
           - temp -> numeric
           - фильтр по 5 и 95 процентилю
        2) extremes_table:
           - по clean_table считаем среднюю температуру за день
           - берём топ-5 hottest и top-5 coldest для каждого года
        """
        stg = meta["staging_table"]
        clean = meta["clean_table"]
        mart = meta["extremes_table"]

        temp_col = meta["temp_col"]
        date_col = meta["date_col"]
        outin_col = meta["outin_col"]

        device_id_expr = "NULL::text"
        room_id_expr = "NULL::text"
        location_expr = "NULL::text"

        hook = PostgresHook(postgres_conn_id="warehouse")
        with hook.get_conn() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                # очищаем clean и mart, чтобы перезапуск DAG был идемпотентным
                cur.execute(f"TRUNCATE TABLE {clean};")
                cur.execute(f"TRUNCATE TABLE {mart};")

                clean_sql = f"""
                WITH base AS (
                  SELECT
                    {device_id_expr} AS device_id,
                    {room_id_expr} AS room_id,
                    {location_expr} AS location,
                    { _quote_ident(outin_col) } AS outin,
                    to_timestamp(
                      { _quote_ident(date_col) },
                      'DD-MM-YYYY HH24:MI'
                    )::date AS noted_date,
                    NULLIF({ _quote_ident(temp_col) }, '')::double precision AS temp_c
                  FROM {stg}
                  WHERE lower({ _quote_ident(outin_col) }) = 'in'
                ),
                bounds AS (
                  SELECT
                    percentile_cont(0.05) WITHIN GROUP (ORDER BY temp_c) AS p05,
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY temp_c) AS p95
                  FROM base
                  WHERE temp_c IS NOT NULL
                )
                INSERT INTO {clean} (device_id, room_id, location, outin, noted_date, temp_c)
                SELECT b.device_id, b.room_id, b.location, b.outin, b.noted_date, b.temp_c
                FROM base b
                CROSS JOIN bounds
                WHERE b.temp_c IS NOT NULL
                  AND b.noted_date IS NOT NULL
                  AND b.temp_c BETWEEN bounds.p05 AND bounds.p95;
                """
                cur.execute(clean_sql)

                mart_sql = f"""
                WITH daily AS (
                  SELECT
                    EXTRACT(YEAR FROM noted_date)::int AS year,
                    noted_date,
                    AVG(temp_c) AS avg_temp_c
                  FROM {clean}
                  GROUP BY 1, 2
                ),
                hottest AS (
                  SELECT
                    year, noted_date, avg_temp_c,
                    'hottest'::text AS kind,
                    ROW_NUMBER() OVER (PARTITION BY year ORDER BY avg_temp_c DESC, noted_date ASC) AS rn
                  FROM daily
                ),
                coldest AS (
                  SELECT
                    year, noted_date, avg_temp_c,
                    'coldest'::text AS kind,
                    ROW_NUMBER() OVER (PARTITION BY year ORDER BY avg_temp_c ASC, noted_date ASC) AS rn
                  FROM daily
                )
                INSERT INTO {mart} (year, noted_date, avg_temp_c, kind, rank_in_kind)
                SELECT year, noted_date, avg_temp_c, kind, rn
                FROM hottest
                WHERE rn <= 5
                UNION ALL
                SELECT year, noted_date, avg_temp_c, kind, rn
                FROM coldest
                WHERE rn <= 5
                ORDER BY year, kind, rn;
                """
                cur.execute(mart_sql)

    csv_path = find_dataset_csv()
    meta = prepare_tables(csv_path)
    load = load_csv_to_staging(csv_path, meta)
    build = transform_and_mart(meta)

    csv_path >> meta >> load >> build
