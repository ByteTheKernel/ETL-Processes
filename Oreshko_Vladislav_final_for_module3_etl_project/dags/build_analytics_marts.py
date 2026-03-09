from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "vladislav oreshko",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="build_analytics_marts",
    default_args=default_args,
    description="Build analytics marts in PostgreSQL",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "marts", "analytics", "postgres"],
) as dag:

    build_user_activity = BashOperator(
        task_id="build_user_activity_mart",
        bash_command="python /opt/airflow/scripts/build_marts.py user_activity",
    )

    build_support_efficiency = BashOperator(
        task_id="build_support_efficiency_mart",
        bash_command="python /opt/airflow/scripts/build_marts.py support_efficiency",
    )

    build_user_activity >> build_support_efficiency