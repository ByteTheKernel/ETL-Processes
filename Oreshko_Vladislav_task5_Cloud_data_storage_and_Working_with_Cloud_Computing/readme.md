# ДЗ 5: Хранение данных в облаке и Работа с облачными вычислениями

**Студент:** Орешко Владислав  
**Дисциплина:** ETL-процессы  
**Темы:** Тема 11 — Хранение данных в облаке · Тема 12 — Работа с облачными вычислениями  

---

## Часть 1. PostgreSQL → Object Storage через Data Transfer

**Задача:** перенести данные из кластера Yandex Managed Service for PostgreSQL в Yandex Object Storage с помощью сервиса Yandex Data Transfer.

### Шаги выполнения

1. Создать кластер-источник Managed Service for PostgreSQL
2. Создать эндпоинт-источник (PostgreSQL) и эндпоинт-приёмник (Object Storage)
3. Создать и запустить трансфер типа «Копирование»
4. Проверить результат в бакете Object Storage
5. Повторно активировать трансфер и проверить обновлённые данные

---

### Скриншот 1 — Кластер PostgreSQL, статус `Alive`

![PostgreSQL cluster alive](screenshots/part1/01_postgresql_cluster_alive.png)

Создан кластер **postgresql_hw5** (версия 17, окружение PRODUCTION), статус — `Alive`.

---

### Скриншот 2 — SQL-редактор: таблица `x_tab` с данными

![x_tab data in SQL editor](screenshots/part1/02_sql_editor_x_tab_data.png)

В базе `db1` создана таблица `x_tab` с 5 строками: `id` (40–44) и `name` (User1–User5).

---

### Скриншот 3 — Data Transfer: список эндпоинтов

![Data Transfer endpoints](screenshots/part1/03_data_transfer_endpoints.png)

Созданы два эндпоинта:
- **pg-source-endpoint** — Источник · PostgreSQL
- **objstorage-endpoint** — Приёмник · Object Storage

---

### Скриншот 4 — Трансфер `pg-to-objstorage-transfer`, статус `Завершён` (первая активация)

![Transfer completed first run](screenshots/part1/04_transfer_completed_first.png)

Трансфер **pg-to-objstorage-transfer** успешно завершился. Источник — `pg-source-endpoint` (PostgreSQL), приёмник — `objstorage-endpoint` (Object Storage).

---

### Скриншот 5 — Бакет Object Storage: файл `public.x_tab.csv`

![Object Storage x_tab csv file](screenshots/part1/05_object_storage_x_tab_csv.png)

В бакете **pg-to-objstorage**, по пути `from_PostgreSQL/public/x_tab/`, появился файл `part-*.csv` размером 84 Б — данные из таблицы `x_tab` успешно выгружены.

---

### Скриншот 6 — Трансфер, статус `Завершён` (повторная активация)

![Transfer completed second run](screenshots/part1/06_transfer_completed_second.png)

Трансфер повторно активирован и снова завершился успешно.

---

### Скриншот 7 — Обновлённый бакет: два CSV-файла после повторной активации

![Object Storage updated csv files](screenshots/part1/07_object_storage_updated_csv.png)

После повторной активации трансфера в папке `x_tab/` появился второй файл `part-*.csv` (67 Б) — данные перегрузились заново.

---

## Часть 2. Обработка данных через Airflow + Data Processing

**Задача:** обработать данные из Yandex Object Storage с помощью Apache Airflow, запустив PySpark-задание на кластере Yandex Data Processing (Dataproc).

### Шаги выполнения

1. Настроить сервисный аккаунт `dataproc` с необходимыми ролями
2. Подготовить бакет: разместить DAG и PySpark-скрипт
3. Создать кластер Hive Metastore
4. Создать кластер Managed Service for Apache Airflow
5. Запустить DAG `DATA_INGEST` и убедиться в успешном выполнении
6. Проверить результаты в бакете (папка `countries/`)

---

### Скриншот 1 — IAM: сервисный аккаунт `dataproc` с ролями

![IAM SA dataproc roles](screenshots/part2/01_iam_sa_dataproc_roles.png)

Сервисный аккаунт **dataproc** настроен с ролями: `dataproc.agent`, `dataproc.provisioner`, `iam.serviceAccounts.user`, `storage.editor`, `managed-airflow.integrationProvider`, `vpc.user`, `managed-metastore.integrationProvider`, `mdb.dataproc.agent`, `dataproc.editor`.

---

### Скриншот 2 — Бакет `airflow-dataproc`: папки `dags/`, `scripts/`, `dataproc/`, `countries/`

![Airflow dataproc bucket folders](screenshots/part2/02_bucket_folders.png)

В бакете **airflow-dataproc** созданы папки: `countries`, `dags`, `dataproc`, `scripts`.

---

### Скриншот 3 — Кластер Hive Metastore, статус `Alive`

![Hive Metastore cluster alive](screenshots/part2/03_hive_metastore_alive.png)

Создан кластер **hive-metastore** (версия 3.1), статус — `Alive`, IP — `10.129.0.12`.

---

### Скриншот 4 — Кластер Airflow, статус `Alive`

![Airflow cluster alive](screenshots/part2/04_airflow_cluster_alive.png)

Создан кластер **airflow** (версия 2.10), статус — `Alive`.

---

### Скриншот 5 — Веб-интерфейс Airflow: DAG `DATA_INGEST` успешно выполнен

![Airflow DAG DATA_INGEST success](screenshots/part2/05_airflow_dag_data_ingest_success.png)

DAG **DATA_INGEST** успешно отработал. Все три задачи завершились со статусом `success`:
- `create-spark-cluster` — `DataprocCreateClusterOperator`
- `pyspark-job` — `DataprocCreatePysparkJobOp...`
- `delete-spark-cluster` — `DataprocDeleteClusterOperator`

---

### Скриншот 6 — Object Storage: папка `countries/` с результатами Spark-задания

![Object Storage countries results](screenshots/part2/06_object_storage_countries_results.png)

В папке `countries/` сохранились результаты PySpark-задания: файл `_SUCCESS` и два файла `.snappy.parquet`.

---

### Скриншот 7 — Содержимое parquet-файла (part-00000): Австралия

![Parquet part0 Australia](screenshots/part2/07_parquet_part0_australia.png)

Первый parquet-файл содержит запись: **Австралия** — Канберра, площадь 7 686 850, население 19 731 984.

---

### Скриншот 8 — Содержимое parquet-файла (part-00001): Австрия

![Parquet part1 Austria](screenshots/part2/08_parquet_part1_austria.png)

Второй parquet-файл содержит запись: **Австрия** — Вена, площадь 83 855, население 7 700 000.

---

## Итог

| Часть | Задача | Статус |
|---|---|---|
| 1 | Кластер PostgreSQL создан и работает | ✅ |
| 1 | Таблица `x_tab` заполнена данными | ✅ |
| 1 | Эндпоинты Data Transfer настроены | ✅ |
| 1 | Первая активация трансфера (Завершён) | ✅ |
| 1 | Данные появились в Object Storage | ✅ |
| 1 | Повторная активация трансфера (Завершён) | ✅ |
| 1 | Обновлённые данные в Object Storage | ✅ |
| 2 | SA `dataproc` с ролями настроен | ✅ |
| 2 | Бакет с папками `dags/`, `scripts/` подготовлен | ✅ |
| 2 | Кластер Hive Metastore создан | ✅ |
| 2 | Кластер Airflow создан | ✅ |
| 2 | DAG `DATA_INGEST` выполнен успешно | ✅ |
| 2 | Результаты сохранены в `countries/` | ✅ |
