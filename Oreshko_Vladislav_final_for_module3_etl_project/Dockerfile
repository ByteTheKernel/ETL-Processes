FROM apache/airflow:3.1.0

USER root
COPY config/airflow-requirements.txt /requirements.txt
USER airflow

RUN pip install --no-cache-dir "apache-airflow==3.1.0" -r /requirements.txt