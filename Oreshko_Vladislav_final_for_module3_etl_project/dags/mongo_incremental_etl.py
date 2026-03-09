from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "vladislav oreshko",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="mongo_incremental_etl",
    default_args=default_args,
    description="Incremental ETL from MongoDB to PostgreSQL staging/dwh",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "mongo", "postgres", "incremental"],
) as dag:

    extract_user_sessions = BashOperator(
        task_id="extract_user_sessions_to_staging",
        bash_command="python /opt/airflow/scripts/mongo_to_staging.py user_sessions",
    )

    extract_event_logs = BashOperator(
        task_id="extract_event_logs_to_staging",
        bash_command="python /opt/airflow/scripts/mongo_to_staging.py event_logs",
    )

    extract_support_tickets = BashOperator(
        task_id="extract_support_tickets_to_staging",
        bash_command="python /opt/airflow/scripts/mongo_to_staging.py support_tickets",
    )

    merge_user_sessions = BashOperator(
        task_id="merge_user_sessions_to_dwh",
        bash_command="python /opt/airflow/scripts/merge_staging_to_dwh.py user_sessions",
    )

    merge_event_logs = BashOperator(
        task_id="merge_event_logs_to_dwh",
        bash_command="python /opt/airflow/scripts/merge_staging_to_dwh.py event_logs",
    )

    merge_support_tickets = BashOperator(
        task_id="merge_support_tickets_to_dwh",
        bash_command="python /opt/airflow/scripts/merge_staging_to_dwh.py support_tickets",
    )

    run_quality_checks = BashOperator(
        task_id="run_quality_checks",
        bash_command="python /opt/airflow/scripts/run_quality_checks.py",
    )

    update_user_sessions_watermark = BashOperator(
        task_id="update_user_sessions_watermark",
        bash_command="python /opt/airflow/scripts/update_watermark.py user_sessions",
    )

    update_event_logs_watermark = BashOperator(
        task_id="update_event_logs_watermark",
        bash_command="python /opt/airflow/scripts/update_watermark.py event_logs",
    )

    update_support_tickets_watermark = BashOperator(
        task_id="update_support_tickets_watermark",
        bash_command="python /opt/airflow/scripts/update_watermark.py support_tickets",
    )

    extract_user_sessions >> merge_user_sessions
    extract_event_logs >> merge_event_logs
    extract_support_tickets >> merge_support_tickets

    [merge_user_sessions, merge_event_logs, merge_support_tickets] >> run_quality_checks

    run_quality_checks >> [
        update_user_sessions_watermark,
        update_event_logs_watermark,
        update_support_tickets_watermark,
    ]