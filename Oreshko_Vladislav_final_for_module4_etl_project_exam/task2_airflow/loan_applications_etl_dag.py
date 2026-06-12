"""
loan_applications_etl_dag.py
Airflow DAG: создаёт кластер DataProc → запускает PySpark → удаляет кластер.
"""

import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

# ── Переменные из Airflow Variables ──────────────────────────────────────────
YC_FOLDER_ID   = Variable.get("yc_folder_id")
YC_SUBNET_ID   = Variable.get("yc_subnet_id")
YC_SA_ID       = Variable.get("yc_sa_id")
S3_BUCKET      = Variable.get("s3_bucket")
SSH_PUBLIC_KEY = Variable.get("yc_ssh_public_key")

# ── Параметры кластера ────────────────────────────────────────────────────────
CLUSTER_NAME = f"dp-loan-etl-{uuid.uuid4().hex[:8]}"
CLUSTER_ZONE = "ru-central1-b"

# ── Пути S3 ──────────────────────────────────────────────────────────────────
PYSPARK_SCRIPT = f"s3a://{S3_BUCKET}/scripts/process_loan_applications.py"
INPUT_PATH     = f"s3a://{S3_BUCKET}/raw/loan_applications.csv"
OUTPUT_PATH    = f"s3a://{S3_BUCKET}/processed/loan_applications/"

# ── DAG defaults ─────────────────────────────────────────────────────────────
default_args = {
    "owner":            "vaoreshko",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

with DAG(
    dag_id="loan_applications_etl",
    description="ETL: loan_applications.csv → DataProc (PySpark) → S3 Parquet",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "dataproc", "loan"],
) as dag:

    # ── 1. Создать кластер DataProc ───────────────────────────────────────────
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",

        folder_id=YC_FOLDER_ID,
        cluster_name=CLUSTER_NAME,
        cluster_description="Temporary cluster for loan ETL",
        zone=CLUSTER_ZONE,
        subnet_id=YC_SUBNET_ID,
        service_account_id=YC_SA_ID,
        ssh_public_keys=SSH_PUBLIC_KEY,

        # Версия DataProc: правильный параметр для нового провайдера
        cluster_image_version="2.0",
        services=["HDFS", "YARN", "SPARK", "TEZ"],

        # Мастер-узел
        masternode_resource_preset="s3-c2-m8",
        masternode_disk_type="network-hdd",
        masternode_disk_size=40,

        # Дата-узел
        datanode_resource_preset="s3-c4-m16",
        datanode_disk_type="network-hdd",
        datanode_disk_size=128,
        datanode_count=1,

        s3_bucket=S3_BUCKET,
        connection_id="yandexcloud_default",
    )

    # ── 2. Запустить PySpark-задание ──────────────────────────────────────────
    run_pyspark = DataprocCreatePysparkJobOperator(
        task_id="run_pyspark_loan_etl",
        cluster_id="{{ task_instance.xcom_pull('create_dataproc_cluster', key='cluster_id') }}",
        main_python_file_uri=PYSPARK_SCRIPT,
        args=[
            "--input",  INPUT_PATH,
            "--output", OUTPUT_PATH,
        ],
        properties={
            "spark.submit.deployMode":      "cluster",
            "spark.executor.memory":        "4g",
            "spark.driver.memory":          "2g",
            "spark.executor.cores":         "2",
            "spark.sql.shuffle.partitions": "8",
            "spark.hadoop.fs.s3a.endpoint": "https://storage.yandexcloud.net",
            "spark.hadoop.fs.s3a.impl":     "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
        connection_id="yandexcloud_default",
    )

    # ── 3. Удалить кластер (всегда, даже при ошибке) ─────────────────────────
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        cluster_id="{{ task_instance.xcom_pull('create_dataproc_cluster', key='cluster_id') }}",
        trigger_rule="all_done",
        connection_id="yandexcloud_default",
    )

    create_cluster >> run_pyspark >> delete_cluster
