from __future__ import annotations

import json
import os
from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

PETS_JSON_PATH = "/data/pets-data.json"
NUTRITION_XML_PATH = "/data/nutrition.xml"

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS etl_raw_files (
    id           BIGSERIAL PRIMARY KEY,
    source       TEXT NOT NULL,
    file_type    TEXT NOT NULL CHECK (file_type IN ('json', 'xml')),
    raw_json     JSONB,
    raw_xml      XML,
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pets (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    species     TEXT,
    birth_year  INT,
    photo       TEXT,
    raw_id      BIGINT NOT NULL REFERENCES etl_raw_files(id) ON DELETE CASCADE,
    UNIQUE (name, raw_id)
);

CREATE TABLE IF NOT EXISTS pet_fav_foods (
    pet_id   BIGINT NOT NULL REFERENCES pets(id) ON DELETE CASCADE,
    food     TEXT NOT NULL,
    PRIMARY KEY (pet_id, food)
);

-- нутриенты часто дробные -> NUMERIC
CREATE TABLE IF NOT EXISTS nutrition_foods (
    id               BIGSERIAL PRIMARY KEY,
    name             TEXT NOT NULL,
    mfr              TEXT,

    serving_g        NUMERIC(10,2),
    calories_total   NUMERIC(10,2),
    calories_fat     NUMERIC(10,2),

    total_fat        NUMERIC(10,2),
    saturated_fat    NUMERIC(10,2),
    cholesterol      NUMERIC(10,2),
    sodium           NUMERIC(10,2),
    carb             NUMERIC(10,2),
    fiber            NUMERIC(10,2),
    protein          NUMERIC(10,2),

    raw_id           BIGINT NOT NULL REFERENCES etl_raw_files(id) ON DELETE CASCADE,
    UNIQUE (name, mfr, raw_id)
);
"""

PARSE_PETS_SQL = """
WITH src AS (
    SELECT id, raw_json
    FROM etl_raw_files
    WHERE file_type = 'json' AND source = 'pets-data.json'
    ORDER BY loaded_at DESC
    LIMIT 1
)
INSERT INTO pets (name, species, birth_year, photo, raw_id)
SELECT
    p->>'name',
    p->>'species',
    (p->>'birthYear')::INT,
    p->>'photo',
    src.id
FROM src,
     LATERAL jsonb_array_elements(src.raw_json->'pets') AS p
ON CONFLICT (name, raw_id) DO NOTHING;

WITH src AS (
    SELECT id, raw_json
    FROM etl_raw_files
    WHERE file_type = 'json' AND source = 'pets-data.json'
    ORDER BY loaded_at DESC
    LIMIT 1
)
INSERT INTO pet_fav_foods (pet_id, food)
SELECT
    p_tbl.id,
    foods.food
FROM src
JOIN pets p_tbl ON p_tbl.raw_id = src.id
JOIN LATERAL (
    SELECT jsonb_array_elements_text(
        (
            SELECT pet->'favFoods'
            FROM jsonb_array_elements(src.raw_json->'pets') AS pet
            WHERE pet->>'name' = p_tbl.name
            LIMIT 1
        )
    ) AS food
) AS foods ON TRUE
ON CONFLICT DO NOTHING;
"""

PARSE_NUTRITION_SQL = """
WITH src AS (
    SELECT id, raw_xml
    FROM etl_raw_files
    WHERE file_type = 'xml' AND source = 'nutrition.xml'
    ORDER BY loaded_at DESC
    LIMIT 1
)
INSERT INTO nutrition_foods (
    name, mfr, serving_g,
    calories_total, calories_fat,
    total_fat, saturated_fat, cholesterol, sodium, carb, fiber, protein,
    raw_id
)
SELECT
    x.name,
    x.mfr,

    NULLIF(regexp_replace(x.serving_g,  '[^0-9.,]+', '', 'g'), '')::NUMERIC,
    NULLIF(regexp_replace(x.calories_total,'[^0-9.,]+', '', 'g'), '')::NUMERIC,
    NULLIF(regexp_replace(x.calories_fat,  '[^0-9.,]+', '', 'g'), '')::NUMERIC,

    NULLIF(regexp_replace(x.total_fat,     '[^0-9.,]+', '', 'g'), '')::NUMERIC,
    NULLIF(regexp_replace(x.saturated_fat, '[^0-9.,]+', '', 'g'), '')::NUMERIC,
    NULLIF(regexp_replace(x.cholesterol,   '[^0-9.,]+', '', 'g'), '')::NUMERIC,
    NULLIF(regexp_replace(x.sodium,        '[^0-9.,]+', '', 'g'), '')::NUMERIC,
    NULLIF(regexp_replace(x.carb,          '[^0-9.,]+', '', 'g'), '')::NUMERIC,
    NULLIF(regexp_replace(x.fiber,         '[^0-9.,]+', '', 'g'), '')::NUMERIC,
    NULLIF(regexp_replace(x.protein,       '[^0-9.,]+', '', 'g'), '')::NUMERIC,

    src.id
FROM src,
XMLTABLE(
    '/nutrition/food'
    PASSING src.raw_xml
    COLUMNS
        name            TEXT PATH 'name',
        mfr             TEXT PATH 'mfr',
        serving_g       TEXT PATH 'serving',
        calories_total  TEXT PATH 'calories/@total',
        calories_fat    TEXT PATH 'calories/@fat',
        total_fat       TEXT PATH 'total-fat',
        saturated_fat   TEXT PATH 'saturated-fat',
        cholesterol     TEXT PATH 'cholesterol',
        sodium          TEXT PATH 'sodium',
        carb            TEXT PATH 'carb',
        fiber           TEXT PATH 'fiber',
        protein         TEXT PATH 'protein'
) AS x
ON CONFLICT (name, mfr, raw_id) DO NOTHING;
"""


def _get_conn():
    uri = os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"]
    return psycopg2.connect(uri)


def run_sql(sql: str) -> None:
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                for stmt in statements:
                    cur.execute(stmt)
    finally:
        conn.close()


def stage_json_to_db(file_path: str) -> None:
    with open(file_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO etl_raw_files (source, file_type, raw_json)
                    VALUES (%s, 'json', %s::jsonb)
                    """,
                    ("pets-data.json", json.dumps(payload, ensure_ascii=False)),
                )
    finally:
        conn.close()


def stage_xml_to_db(file_path: str) -> None:
    with open(file_path, "r", encoding="utf-8") as f:
        xml_text = f.read()

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO etl_raw_files (source, file_type, raw_xml)
                    VALUES (%s, 'xml', %s::xml)
                    """,
                    ("nutrition.xml", xml_text),
                )
    finally:
        conn.close()


with DAG(
    dag_id="load_files_and_parse_in_db",
    start_date=datetime(2026, 1, 27),
    schedule=None,
    catchup=False,
    tags=["etl", "postgres", "json", "xml"],
) as dag:

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=run_sql,
        op_kwargs={"sql": CREATE_TABLES_SQL},
    )

    load_json_raw = PythonOperator(
        task_id="load_json_raw_to_db",
        python_callable=stage_json_to_db,
        op_kwargs={"file_path": PETS_JSON_PATH},
    )

    load_xml_raw = PythonOperator(
        task_id="load_xml_raw_to_db",
        python_callable=stage_xml_to_db,
        op_kwargs={"file_path": NUTRITION_XML_PATH},
    )

    parse_pets_in_db = PythonOperator(
        task_id="parse_pets_in_db",
        python_callable=run_sql,
        op_kwargs={"sql": PARSE_PETS_SQL},
    )

    parse_nutrition_in_db = PythonOperator(
        task_id="parse_nutrition_in_db",
        python_callable=run_sql,
        op_kwargs={"sql": PARSE_NUTRITION_SQL},
    )

    create_tables >> [load_json_raw, load_xml_raw]
    load_json_raw >> parse_pets_in_db
    load_xml_raw >> parse_nutrition_in_db
