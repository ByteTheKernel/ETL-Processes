import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_DP_AZ = 'ru-central1-b'
YC_DP_SSH_PUBLIC_KEY = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCoIjn+mKH8VxawThww/NyOSjM0kV4erCfOjjvWXPlwqvErwDwOpDMi/RG+GG+oPbB3EPQnIGMn6mdbxLl7Fp8CBhFvAEIcdDvnsPsJrf3mCWATw/9dBQbNtjAMCREB8rIbDxygJzKYCnG12D1HlNeRaiE3J6k6WXLyNUjEpSP5Wb9XDrxpXQIYnHvBhdnc73F7fFWHeKsVnG2ZijYKk4pSSeVvV3AiowmEJNknYbGyEvgzYeN1QW2ev3mAjnW/jU9QFMQpC8JJf/ZXQMXVWZQRRn4CyD6G+WARh/QoSYJ+ovBUHLeRqU8E95v3YAOiBgm0yczOXcy267MFOlaJSFtX vlado@LAPTOP-6SG7DU4Q'
YC_DP_SUBNET_ID = 'e2ljclfpr830vrkvs8jj'
YC_DP_SA_ID = 'ajeka39dhdop1oes5dof'
YC_SOURCE_BUCKET = 'airflow-dataproc'
YC_DP_LOGS_BUCKET = 'airflow-dataproc'

YC_DP_CLUSTER_NAME = f'tmp-dp-{uuid.uuid4()}'
YC_DP_CLUSTER_DESC = 'Temporary cluster for Airflow DAG DATA_INGEST'

with DAG(
    dag_id='DATA_INGEST',
    start_date=datetime.datetime(2024, 5, 25),
    schedule_interval=None,
    tags=['data-processing-and-airflow'],
    catchup=False,
) as dag:

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='create-spark-cluster',
        zone=YC_DP_AZ,
        cluster_name=YC_DP_CLUSTER_NAME,
        cluster_description=YC_DP_CLUSTER_DESC,
        subnet_id=YC_DP_SUBNET_ID,
        service_account_id=YC_DP_SA_ID,
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        s3_bucket=YC_DP_LOGS_BUCKET,
        enable_ui_proxy=False,
        cluster_image_version='2.1',
        masternode_resource_preset='s3-c2-m8',
        masternode_disk_size=20,
        masternode_disk_type='network-ssd',
        datanode_resource_preset='s3-c4-m16',
        datanode_disk_size=20,
        datanode_disk_type='network-ssd',
        datanode_count=1,
        services=['HDFS', 'YARN', 'SPARK'],
    )

    pyspark_job = DataprocCreatePysparkJobOperator(
        task_id='pyspark-job',
        main_python_file_uri=f's3a://{YC_SOURCE_BUCKET}/scripts/create-table.py',
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='delete-spark-cluster',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> pyspark_job >> delete_spark_cluster
