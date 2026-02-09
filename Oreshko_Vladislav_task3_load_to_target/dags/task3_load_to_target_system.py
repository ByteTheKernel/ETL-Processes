from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Dict, List, Tuple

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime

DATA_DIR = Path("/data")

PIPELINE_NAME = "task3_iot_load_to_target"
DEFAULT_LOOKBACK_DAYS = 3


# -------------------- helpers --------------------

def _sanitize_col(name: str) -> str:
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
            f"Положите CSV датасета в ./data"
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
    temp_candidates = ["temp", "temperature", "temp_c", "temperature_c"]
    date_candidates = ["noted_date", "date", "datetime", "timestamp", "time"]
    outin_candidates = ["out_in", "outin", "in_out"]

    def find_any(cands: List[str]) -> str | None:
        for c in cands:
            if c in sanitized:
                return c
        return None

    temp_col = find_any(temp_candidates)
    date_col = find_any(date_candidates)
    outin_col = find_any(outin_candidates)

    if not temp_col:
        raise ValueError(f"Не найдена колонка температуры. Колонки: {sanitized}")
    if not date_col:
        raise ValueError(f"Не найдена колонка даты. Колонки: {sanitized}")
    if not outin_col:
        raise ValueError(f"Не найдена колонка out/in. Колонки: {sanitized}")

    return temp_col, date_col, outin_col


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# -------------------- DAG --------------------

with DAG(
    dag_id="task3_load_to_target_system",
    start_date=datetime(2026, 2, 9),
    schedule=None,
    catchup=False,
    tags=["etl", "hw3", "load", "full", "incremental", "watermark"],
) as dag:

    @task
    def find_dataset_csv() -> str:
        return str(_pick_first_csv(DATA_DIR))

    @task
    def prepare_stg_and_clean_tables(csv_path: str) -> Dict[str, str]:
        """
        Создаём:
        - etl.stg_iot_temperature (все колонки TEXT)
        - etl.dwh_iot_temperature_clean (результат трансформации из ДЗ2)
        """
        p = Path(csv_path)
        header = _read_header(p)
        sanitized = [_sanitize_col(h) for h in header]

        temp_col, date_col, outin_col = _infer_required_columns(sanitized)

        stg = "etl.stg_iot_temperature"
        clean = "etl.dwh_iot_temperature_clean"

        cols_ddl = ",\n  ".join(f"{_quote_ident(c)} text" for c in sanitized)

        hook = PostgresHook(postgres_conn_id="warehouse")
        with hook.get_conn() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS etl;")

                cur.execute(f"DROP TABLE IF EXISTS {stg};")
                cur.execute(f"CREATE TABLE {stg} ({cols_ddl});")

                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {clean} (
                      device_id text,
                      room_id text,
                      location text,
                      outin text,
                      noted_date date,
                      temp_c double precision
                    );
                    """
                )

        return {
            "staging_table": stg,
            "clean_table": clean,
            "temp_col": temp_col,
            "date_col": date_col,
            "outin_col": outin_col,
            "sanitized_header": ",".join(sanitized),
        }

    @task
    def load_csv_to_staging(csv_path: str, meta: Dict[str, str]) -> None:
        """
        COPY CSV -> staging
        """
        p = Path(csv_path)
        stg = meta["staging_table"]
        sanitized = meta["sanitized_header"].split(",")

        original_header = _read_header(p)
        original_sanitized = [_sanitize_col(h) for h in original_header]
        if original_sanitized != sanitized:
            raise ValueError(
                "Несовпадение header. "
                f"original_sanitized={original_sanitized} sanitized={sanitized}"
            )

        hook = PostgresHook(postgres_conn_id="warehouse")
        with hook.get_conn() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cols_sql = ", ".join(_quote_ident(c) for c in sanitized)
                copy_sql = f"COPY {stg} ({cols_sql}) FROM STDIN WITH (FORMAT csv, HEADER true)"
                with p.open("r", encoding="utf-8") as f:
                    cur.copy_expert(copy_sql, f)

    @task
    def build_clean_layer(meta: Dict[str, str]) -> None:
        """
        трансформации ДЗ2:
        - out_in='in'
        - noted_date (DD-MM-YYYY HH24:MI) -> date
        - temp -> double
        - фильтр по 5 и 95 процентилю
        """
        stg = meta["staging_table"]
        clean = meta["clean_table"]
        temp_col = meta["temp_col"]
        date_col = meta["date_col"]
        outin_col = meta["outin_col"]

        hook = PostgresHook(postgres_conn_id="warehouse")
        with hook.get_conn() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {clean};")

                # ВАЖНО: у датасета формат "08-12-2018 09:30" -> это DD-MM-YYYY
                clean_sql = f"""
                WITH base AS (
                  SELECT
                    NULL::text AS device_id,
                    NULL::text AS room_id,
                    NULL::text AS location,
                    {_quote_ident(outin_col)} AS outin,
                    to_timestamp({_quote_ident(date_col)}, 'DD-MM-YYYY HH24:MI')::date AS noted_date,
                    NULLIF({_quote_ident(temp_col)}, '')::double precision AS temp_c
                  FROM {stg}
                  WHERE lower({_quote_ident(outin_col)}) = 'in'
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

    @task
    def prepare_target_objects() -> None:
        """
        Target + watermark (для full/incremental).
        """
        hook = PostgresHook(postgres_conn_id="warehouse")
        with hook.get_conn() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS etl;")

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS etl.etl_watermarks (
                        pipeline_name text PRIMARY KEY,
                        last_watermark_date date,
                        updated_at timestamp NOT NULL DEFAULT now()
                    );
                    """
                )

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS etl.dwh_iot_temperature_target (
                        row_hash text PRIMARY KEY,
                        device_id text,
                        room_id text,
                        location text,
                        outin text,
                        noted_date date,
                        temp_c double precision,
                        load_dttm timestamp NOT NULL DEFAULT now()
                    );
                    """
                )

                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_iot_target_noted_date ON etl.dwh_iot_temperature_target(noted_date);"
                )

                cur.execute(
                    """
                    INSERT INTO etl.etl_watermarks (pipeline_name, last_watermark_date)
                    VALUES (%s, NULL)
                    ON CONFLICT (pipeline_name) DO NOTHING;
                    """,
                    (PIPELINE_NAME,),
                )

    @task
    def load_mode_router() -> str:
        """
        Управление режимом через Airflow Variables:
        - task3_load_mode: full | incremental
        - task3_iot_lookback_days: 3 (по умолчанию)
        """
        return Variable.get("task3_load_mode", "incremental").strip().lower()

    @task
    def full_load(meta: Dict[str, str]) -> None:
        """
        FULL: чистим target и заливаем всё из clean. Watermark = max(noted_date).
        """
        clean = meta["clean_table"]

        hook = PostgresHook(postgres_conn_id="warehouse")
        with hook.get_conn() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE etl.dwh_iot_temperature_target;")

                cur.execute(
                    f"""
                    INSERT INTO etl.dwh_iot_temperature_target (
                        row_hash, device_id, room_id, location, outin, noted_date, temp_c
                    )
                    SELECT DISTINCT
                        md5(
                            coalesce(device_id,'') || '|' ||
                            coalesce(room_id,'')   || '|' ||
                            coalesce(location,'')  || '|' ||
                            coalesce(outin,'')     || '|' ||
                            coalesce(noted_date::text,'') || '|' ||
                            coalesce(temp_c::text,'')
                        ) AS row_hash,
                        device_id, room_id, location, outin, noted_date, temp_c
                    FROM {clean}
                    WHERE noted_date IS NOT NULL
                      AND temp_c IS NOT NULL;
                    """
                )

                cur.execute(
                    """
                    UPDATE etl.etl_watermarks
                    SET last_watermark_date = (SELECT MAX(noted_date) FROM etl.dwh_iot_temperature_target),
                        updated_at = now()
                    WHERE pipeline_name = %s;
                    """,
                    (PIPELINE_NAME,),
                )

    @task
    def incremental_load(meta: Dict[str, str], mode: str) -> None:
        """
        INCR:
        - берём watermark
        - берём окно lookback_days назад (защита от поздних данных)
        - upsert по row_hash
        - обновляем watermark
        Если watermark пустой (первый запуск) — делаем initial-load (без truncate).
        """
        clean = meta["clean_table"]
        lookback_days = int(Variable.get("task3_iot_lookback_days", DEFAULT_LOOKBACK_DAYS))

        hook = PostgresHook(postgres_conn_id="warehouse")
        with hook.get_conn() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT last_watermark_date FROM etl.etl_watermarks WHERE pipeline_name = %s;",
                    (PIPELINE_NAME,),
                )
                last_wm = cur.fetchone()[0]  # date | None

                # если mode=full — выходим (full делается отдельной таской)
                if mode == "full":
                    return

                if last_wm is None:
                    # initial incremental: просто upsert всего
                    cur.execute(
                        f"""
                        INSERT INTO etl.dwh_iot_temperature_target (
                            row_hash, device_id, room_id, location, outin, noted_date, temp_c
                        )
                        SELECT DISTINCT
                            md5(
                                coalesce(device_id,'') || '|' ||
                                coalesce(room_id,'')   || '|' ||
                                coalesce(location,'')  || '|' ||
                                coalesce(outin,'')     || '|' ||
                                coalesce(noted_date::text,'') || '|' ||
                                coalesce(temp_c::text,'')
                            ) AS row_hash,
                            device_id, room_id, location, outin, noted_date, temp_c
                        FROM {clean}
                        WHERE noted_date IS NOT NULL
                          AND temp_c IS NOT NULL
                        ON CONFLICT (row_hash) DO UPDATE
                        SET load_dttm = now();
                        """
                    )
                else:
                    cur.execute(
                        f"""
                        WITH src AS (
                            SELECT
                                md5(
                                    coalesce(device_id,'') || '|' ||
                                    coalesce(room_id,'')   || '|' ||
                                    coalesce(location,'')  || '|' ||
                                    coalesce(outin,'')     || '|' ||
                                    coalesce(noted_date::text,'') || '|' ||
                                    coalesce(temp_c::text,'')
                                ) AS row_hash,
                                device_id, room_id, location, outin, noted_date, temp_c
                            FROM {clean}
                            WHERE noted_date IS NOT NULL
                              AND temp_c IS NOT NULL
                              AND noted_date >= (%s::date - (%s || ' day')::interval)
                        )
                        INSERT INTO etl.dwh_iot_temperature_target (
                            row_hash, device_id, room_id, location, outin, noted_date, temp_c
                        )
                        SELECT DISTINCT row_hash, device_id, room_id, location, outin, noted_date, temp_c
                        FROM src
                        ON CONFLICT (row_hash) DO UPDATE
                        SET load_dttm = now();
                        """,
                        (last_wm, lookback_days),
                    )

                cur.execute(
                    """
                    UPDATE etl.etl_watermarks
                    SET last_watermark_date = (SELECT MAX(noted_date) FROM etl.dwh_iot_temperature_target),
                        updated_at = now()
                    WHERE pipeline_name = %s;
                    """,
                    (PIPELINE_NAME,),
                )

    # ---- graph ----
    csv_path = find_dataset_csv()
    meta = prepare_stg_and_clean_tables(csv_path)
    load_stg = load_csv_to_staging(csv_path, meta)
    clean_layer = build_clean_layer(meta)
    prep_target = prepare_target_objects()
    mode = load_mode_router()

    do_full = full_load(meta)
    do_incr = incremental_load(meta, mode)

    csv_path >> meta >> load_stg >> clean_layer >> prep_target >> mode
    mode >> do_full
    mode >> do_incr
