# ETL-процессы — Итоговое задание. Модуль 4

**Студент:** Владислав Орешко (vaoreshko@edu.hse.ru)  

---

## Содержание

- [Обзор проекта](#обзор-проекта)
- [Задание 1 — Yandex DataTransfer: YDB → Object Storage](#задание-1--yandex-datatransfer-ydb--object-storage)
- [Задание 2 — Автоматизация DataProc с Apache Airflow](#задание-2--автоматизация-dataproc-с-apache-airflow)
- [Задание 3 — Apache Kafka + PySpark](#задание-3--apache-kafka--pyspark)
- [Задание 4 — Визуализация в DataLens](#задание-4--визуализация-в-datalens)
- [Структура репозитория](#структура-репозитория)

---

## Обзор проекта

В рамках задания реализован полноценный ETL-пайплайн на базе облачной платформы Yandex Cloud. Данные проходят путь от исходных источников (YDB, CSV, Kafka) через обработку (DataTransfer, PySpark, Airflow) до визуализации в DataLens.

**Используемая инфраструктура:**

| Сервис | Назначение |
|---|---|
| Yandex Managed Service for YDB | Источник данных (транзакции) |
| Yandex Data Transfer | Репликация YDB → Object Storage |
| Yandex Object Storage (S3) | Централизованное хранилище данных |
| Yandex Data Processing (DataProc) | Выполнение PySpark-заданий |
| Yandex Managed Service for Apache Airflow | Оркестрация ETL-пайплайна |
| Yandex Managed Service for Apache Kafka | Потоковая передача данных |
| Yandex Query | SQL-движок для чтения данных из S3 |
| Yandex DataLens | Визуализация и дашборды |

**Бакет S3:** `etl-exam`

---

## Задание 1 — Yandex DataTransfer: YDB → Object Storage

### Цель

Перенести данные из Managed Service for YDB в объектное хранилище Object Storage с помощью сервиса Data Transfer.

### Подготовка данных

Сгенерированы данные о звонках колл-центра — таблица `transactions_v2` (287 000 строк, ~31 МБ). Структура:

```
call_id, call_time, client_id, region_code, campaign_type,
call_status, client_response, duration_sec, follow_up_required
```

Пример записи:
```
call_20260501_001, 2026-05-01 11:42:15, client_4412, DE-HE,
credit_card_offer, answered, interested, 184, true
```

### Создание YDB и загрузка данных

Создана Serverless YDB `etl-exam-db`. Таблица создана через YQL-скрипт [`task1_ydb_transfer/01_create_table.yql`](task1_ydb_transfer/01_create_table.yql):

```sql
CREATE TABLE transactions_v2 (
    call_id          Utf8,
    call_time        Utf8,
    client_id        Utf8,
    region_code      Utf8,
    campaign_type    Utf8,
    call_status      Utf8,
    client_response  Utf8,
    duration_sec     Int32,
    follow_up_required Utf8,
    PRIMARY KEY (call_id)
);
```

Данные загружены через Python SDK (8 потоков параллельно) — скрипт [`task1_ydb_transfer/load_via_ydb_sdk.py`](task1_ydb_transfer/load_via_ydb_sdk.py).

![Создание YDB](task1_ydb_transfer/task1_screenshots/01_ydb_create_form.png)

![YDB запущена](task1_ydb_transfer/task1_screenshots/02_ydb_running.png)

![Создание таблицы YQL](task1_ydb_transfer/task1_screenshots/03_create_table_yql.png)

![287 000 записей загружено](task1_ydb_transfer/task1_screenshots/04_ydb_count_287000.png)

### Настройка трансфера DataTransfer

Создано два эндпоинта:

**Источник (YDB):**

![Эндпоинт YDB — форма](task1_ydb_transfer/task1_screenshots/05_endpoint_ydb_source_form.png)

![Эндпоинт YDB — создан](task1_ydb_transfer/task1_screenshots/06_endpoint_ydb_source_created.png)

**Приёмник (Object Storage):**
- Бакет: `etl-exam`
- Формат: Parquet
- Папка: `ydb-transfer`

![Эндпоинт S3 — форма](task1_ydb_transfer/task1_screenshots/07_endpoint_s3_target_form.png)

![Эндпоинт S3 — формат Parquet](task1_ydb_transfer/task1_screenshots/08_endpoint_s3_parquet.png)

![Эндпоинт S3 — папка](task1_ydb_transfer/task1_screenshots/09_endpoint_s3_folder.png)

![Оба эндпоинта созданы](task1_ydb_transfer/task1_screenshots/10_both_endpoints_created.png)

### Запуск трансфера

Создан трансфер типа **Копирование** и активирован:

![Форма создания трансфера](task1_ydb_transfer/task1_screenshots/11_transfer_create_form.png)

![Трансфер выполняется](task1_ydb_transfer/task1_screenshots/12_transfer_waiting.png)

![Трансфер завершён](task1_ydb_transfer/task1_screenshots/13_transfer_completed.png)

![Мониторинг трансфера](task1_ydb_transfer/task1_screenshots/14_transfer_monitoring.png)

![Логи — 287 000 строк](task1_ydb_transfer/task1_screenshots/15_transfer_logs_287000.png)

![Логи — завершено](task1_ydb_transfer/task1_screenshots/16_transfer_logs_finished.png)

### Результат

Данные успешно перенесены в `s3://etl-exam/ydb-transfer/transactions_v2/` в формате Parquet (~28.42 МБ).

![Папка ydb-transfer в S3](task1_ydb_transfer/task1_screenshots/17_s3_ydb_transfer_folder.png)

![Папка transactions_v2](task1_ydb_transfer/task1_screenshots/18_s3_transactions_v2_folder.png)

![Parquet файл 28 МБ](task1_ydb_transfer/task1_screenshots/19_s3_parquet_file_28mb.png)

Проверочные запросы к YDB сохранены в [`task1_ydb_transfer/03_verify_queries.yql`](task1_ydb_transfer/03_verify_queries.yql).

---

## Задание 2 — Автоматизация DataProc с Apache Airflow

### Цель

Реализовать автоматизированный ETL-пайплайн: DAG в Airflow создаёт кластер DataProc, запускает PySpark-задание обработки CSV-файла и удаляет кластер.

### Подготовка данных

Сгенерирован файл кредитных заявок `loan_applications.csv` (~55.61 МБ, 500 000 строк):

```
application_id, event_time, customer_id, region_code, product_type,
requested_amount, term_months, credit_score, risk_level, decision_status,
approved_amount, channel, employee_review_flag, processing_time_sec
```

### Создание кластера Airflow

Создан кластер Managed Service for Apache Airflow `airflow-etl` (версия 2.10):
- Сервисный аккаунт: `dataproc`
- Бакет DAG-файлов: `etl-exam`

![Создание Airflow — форма 1](task2_airflow/task2_screenshots/01_airflow_create_form_1.png)

![Создание Airflow — сеть](task2_airflow/task2_screenshots/02_airflow_create_form_2_network.png)

![Создание Airflow — воркеры](task2_airflow/task2_screenshots/03_airflow_create_form_3_workers.png)

![Создание Airflow — воркеры 2](task2_airflow/task2_screenshots/04_airflow_create_form_4_workers2.png)

![Создание Airflow — бакет DAG](task2_airflow/task2_screenshots/05_airflow_create_form_5_dags_bucket.png)

![Создание Airflow — логирование](task2_airflow/task2_screenshots/06_airflow_create_form_6_logging.png)

![Кластер Airflow работает](task2_airflow/task2_screenshots/13_airflow_cluster_alive.png)

### PySpark-задание

Скрипт [`task2_airflow/process_loan_applications.py`](task2_airflow/process_loan_applications.py) выполняет:
- Чтение CSV из S3
- Парсинг и обогащение данных (добавление `event_year`, `event_month`, `credit_score_band`, `is_approved`, `approval_rate`)
- Запись результата в Parquet с партиционированием по `event_year` / `event_month`

### DAG-файл

Файл [`task2_airflow/loan_applications_etl_dag.py`](task2_airflow/loan_applications_etl_dag.py) реализует три шага:

```
create_dataproc_cluster → run_pyspark_job → delete_dataproc_cluster
```

Параметры кластера DataProc:
- Версия образа: **2.0** (не 2.1 — конфликт Kafka JAR с SLF4J)
- Зона: `ru-central1-b`
- Мастер: `s3-c2-m8`, Data: `s3-c4-m16`

Загрузка скриптов и DAG-файла в S3:

![Структура бакета S3](task2_airflow/task2_screenshots/08_s3_bucket_structure.png)

![DAG-файл в S3](task2_airflow/task2_screenshots/09_s3_dags_file.png)

### Настройка Airflow Variables

В веб-интерфейсе Airflow созданы переменные:

| Переменная | Значение |
|---|---|
| `yc_folder_id` | ID каталога |
| `yc_subnet_id` | ID подсети |
| `yc_sa_id` | ID сервисного аккаунта |
| `s3_bucket` | `etl-exam` |
| `yc_ssh_public_key` | SSH-ключ |

![Airflow Variables](task2_airflow/task2_screenshots/10_airflow_variables.png)

### Запуск DAG

![DAG загружен](task2_airflow/task2_screenshots/11_dag_loaded_success.png)

![DAG выполняется](task2_airflow/task2_screenshots/12_dag_running.png)

![Кластер DataProc создан и работает](task2_airflow/task2_screenshots/14_dataproc_cluster_alive.png)

![Кластер DataProc удалён](task2_airflow/task2_screenshots/15_dataproc_cluster_deleted.png)

### Результат

Время выполнения DAG: **~3 мин 11 сек**. Данные сохранены в `s3://etl-exam/processed/loan_applications/` с партиционированием:

![Папка processed в S3](task2_airflow/task2_screenshots/16_s3_processed_folder.png)

![Папка loan_applications](task2_airflow/task2_screenshots/17_s3_loan_applications_folder.png)

![Партиции event_year/event_month](task2_airflow/task2_screenshots/18_s3_partitions.png)

![Parquet файл результата](task2_airflow/task2_screenshots/19_s3_parquet_file.png)

---

## Задание 3 — Apache Kafka + PySpark

### Цель

Настроить потоковую передачу данных через Apache Kafka, обработать JSON-сообщения с помощью PySpark и разложить их в плоскую таблицу.

### Архитектура

```
JSON-сообщения → Kafka topic (loan-applications) → PySpark (DataProc) → Parquet (S3)
```

### Создание кластера Kafka

Создан кластер Managed Service for Apache Kafka `kafka-etl`:
- Версия: 3.9
- Зона: `ru-central1-b`
- Брокер: `rc1b-ovoaudq9ut7cf61s.mdb.yandexcloud.net:9091`
- Протокол: SASL_SSL

![Создание Kafka — форма 1](task3_kafka/task3_screenshots/01_kafka_create_form1.png)

![Создание Kafka — диск](task3_kafka/task3_screenshots/02_kafka_create_form2_disk.png)

![Создание Kafka — сеть](task3_kafka/task3_screenshots/03_kafka_create_form3_network.png)

![Создание Kafka — завершение](task3_kafka/task3_screenshots/04_kafka_create_form4_bottom.png)

Создан топик `loan-applications` (1 раздел, репликация 1):

![Создание топика](task3_kafka/task3_screenshots/05_kafka_topic_create.png)

Создан пользователь `kafka-user` с правами PRODUCER + CONSUMER на все топики:

![Создание пользователя Kafka](task3_kafka/task3_screenshots/06_kafka_user_create.png)

![FQDN брокеров](task3_kafka/task3_screenshots/07_kafka_hosts_fqdn.png)

### Отправка сообщений

Получен и конвертирован CA-сертификат Yandex Cloud в формат JKS для SASL_SSL:

![Cloud Shell — получение JKS](task3_kafka/task3_screenshots/08_cloud_shell_jks.png)

Через скрипт [`task3_kafka/send_to_kafka.py`](task3_kafka/send_to_kafka.py) отправлено **60 000 сообщений** (~21.65 МБ) в топик `loan-applications`.

Структура каждого JSON-сообщения:
```json
{
  "application_id": "loan_784512",
  "customer": {"customer_id": "cust_441", "region": "DE-HE"},
  "loan": {"amount": 15000, "term_months": 36},
  "scoring": {"score": 712, "risk_level": "medium"},
  "documents": [{"type": "passport", "status": "verified"}],
  "decision_status": "manual_review",
  "submitted_at": "2026-05-01T10:15:11Z"
}
```

![60 000 сообщений отправлено](task3_kafka/task3_screenshots/09_kafka_60k_messages_sent.png)

### Настройка DataProc для PySpark

Загружены Kafka JAR-файлы в `s3://etl-exam/jars/` (вместо Maven — интернет заблокирован группой безопасности DataProc):

![Папка jars в S3](task3_kafka/task3_screenshots/10_s3_jars_folder.png)

Создан кластер DataProc `dp-kafka-etl` (версия 2.0):

![DataProc версия 2.0](task3_kafka/task3_screenshots/15_dataproc_version_20.png)

![Создание DataProc — SA](task3_kafka/task3_screenshots/12_dataproc_create_form2_sa.png)

![Создание DataProc — подкластеры](task3_kafka/task3_screenshots/13_dataproc_create_form3_subclusters.png)

![Создание DataProc — завершение](task3_kafka/task3_screenshots/14_dataproc_create_form4_bottom.png)

![DataProc кластер запущен](task3_kafka/task3_screenshots/16_dataproc_cluster_alive.png)

### PySpark-задание

Скрипт [`task3_kafka/process_kafka_messages.py`](task3_kafka/process_kafka_messages.py):
- Читает JSON-сообщения
- Разворачивает вложенную структуру в плоскую таблицу
- Добавляет поля `is_approved`, `event_date`, `event_month`, `event_year`
- Выполняет агрегацию по региону и уровню риска
- Сохраняет результат в Parquet

![Форма создания задания DataProc](task3_kafka/task3_screenshots/17_dataproc_job_create.png)

![JARs загружены в Cloud Shell](task3_kafka/task3_screenshots/18_cloud_shell_jars_uploaded.png)

![Задание с JARs](task3_kafka/task3_screenshots/19_job_create_with_jars.png)

![Задание в режиме S3](task3_kafka/task3_screenshots/20_job_s3_mode_create.png)

### Результат

Задание выполнено за **29 секунд** со статусом Done. Результаты сохранены в `s3://etl-exam/processed/kafka_results/`:

![Папка kafka_results в S3](task3_kafka/task3_screenshots/21_s3_kafka_results_folder.png)

![Задание завершено за 29 сек](task3_kafka/task3_screenshots/22_job_done_29sec.png)

Плоская схема результирующей таблицы:

| Поле | Тип | Описание |
|---|---|---|
| `application_id` | String | ID заявки |
| `customer_id` | String | ID клиента |
| `region` | String | Регион |
| `loan_amount` | Int64 | Сумма кредита |
| `term_months` | Int32 | Срок (месяцы) |
| `credit_score` | Int32 | Кредитный скор |
| `risk_level` | String | Уровень риска |
| `decision_status` | String | Решение |
| `doc_type` | String | Тип документа |
| `doc_status` | String | Статус документа |
| `is_approved` | Int32 | Одобрено (0/1) |
| `event_month` | String | Месяц события |
| `event_year` | Int32 | Год события |

---

## Задание 4 — Визуализация в DataLens

### Цель

Построить дашборд в Yandex DataLens, визуализирующий результаты всех трёх заданий.

### Архитектура подключения

Данные читаются через **Yandex Query** напрямую из S3 в формате Parquet и CSV без промежуточного хранения.

```
DataLens → Yandex Query → S3 (etl-exam)
                ├── raw/transactions_v2.csv          (Задание 1)
                ├── processed/loan_applications/     (Задание 2)
                └── processed/kafka_results/flat/    (Задание 3)
```

Создано подключение `etl-exam подключение` типа Yandex Query.

### Чарты дашборда

Создано 5 чартов на основе SQL-запросов к Yandex Query:

**Задание 1 — Статусы звонков** ([`chart4_call_statuses.sql`](task4_datalens/chart4_call_statuses.sql))

Визуализирует распределение звонков из `transactions_v2` по статусам (`answered`, `no_answer`, `busy`, `voicemail`, `callback_requested`).

![Чарт: статусы звонков](task4_datalens/task4_screenshots/06_chart_call_statuses_task1.png)

**Задание 2 — Заявки по уровню риска** ([`chart1_risk_level.sql`](task4_datalens/chart1_risk_level.sql))

Сравнение общего числа заявок и одобренных по уровням риска (high / low / medium).

![Чарт: заявки по уровню риска](task4_datalens/task4_screenshots/03_chart_risk_level.png)

**Задание 2 — Решения по типу продукта** ([`chart2_product_decisions.sql`](task4_datalens/chart2_product_decisions.sql))

Распределение статусов решений (`approved`, `declined`, `manual_review`, `pending`) по каждому типу кредитного продукта.

![Чарт: решения по типу продукта](task4_datalens/task4_screenshots/04_chart_product_decisions.png)

**Задание 2 — Средний кредитный скор по каналу** ([`chart3_avg_score_channel.sql`](task4_datalens/chart3_avg_score_channel.sql))

Средний credit score клиентов, подавших заявку через каждый канал (mobile, web, branch, partner, call_center).

![Чарт: средний скор по каналу](task4_datalens/task4_screenshots/05_chart_avg_score_channel.png)

**Задание 3 — Kafka: риск vs решение** ([`chart5_kafka_risk.sql`](task4_datalens/chart5_kafka_risk.sql))

Распределение решений по уровням риска для обработанных Kafka-сообщений.

![Чарт: Kafka риск vs решение](task4_datalens/task4_screenshots/07_chart_kafka_risk_task3.png)

### Итоговый дашборд

Все 5 чартов собраны в единый дашборд **ETL Exam Dashboard**:

![ETL Exam Dashboard](task4_datalens/task4_screenshots/01_dashboard_final.png)

![Воркбук с объектами](task4_datalens/task4_screenshots/02_workbook_overview.png)

### Особенности реализации

- Поля `submitted_at` (timestamp[ns]) и `event_date` (date32) исключены из схемы — Yandex Query не поддерживает эти типы из Parquet
- При чтении папки `kafka_results/flat/` файл `_SUCCESS` (Spark-маркер) вызывал ошибку — решено указанием точного пути к `.snappy.parquet` файлу

---

## Структура репозитория

```
├── data/
│   ├── transactions_v2.csv          # Данные транзакций (Задание 1)
│   ├── loan_applications.csv        # Кредитные заявки (Задание 2)
│   └── kafka_messages.jsonl         # JSON-сообщения для Kafka (Задание 3)
│
├── data_generation/
│   ├── generate_transactions_v2.py  # Генератор транзакций
│   ├── generate_loan_applications.py
│   └── generate_kafka_messages.py
│
├── task1_ydb_transfer/
│   ├── 01_create_table.yql          # Создание таблицы в YDB
│   ├── 02_upsert_sample.yql         # Пример вставки данных
│   ├── 03_verify_queries.yql        # Проверочные запросы
│   ├── load_via_ydb_sdk.py          # Загрузка через YDB SDK
│   └── task1_screenshots/
│
├── task2_airflow/
│   ├── process_loan_applications.py # PySpark ETL скрипт
│   ├── loan_applications_etl_dag.py # Airflow DAG
│   └── task2_screenshots/
│
├── task3_kafka/
│   ├── send_to_kafka.py             # Отправка сообщений в Kafka
│   ├── process_kafka_messages.py    # PySpark обработка JSON
│   └── task3_screenshots/
│
└── task4_datalens/
    ├── chart1_risk_level.sql
    ├── chart2_product_decisions.sql
    ├── chart3_avg_score_channel.sql
    ├── chart4_call_statuses.sql
    ├── chart5_kafka_risk.sql
    └── task4_screenshots/
```
