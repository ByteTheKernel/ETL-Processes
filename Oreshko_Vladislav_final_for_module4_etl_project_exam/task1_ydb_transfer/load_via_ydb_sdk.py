"""
Задание 1: Массовая загрузка данных из CSV в YDB через Python SDK.

Установка:
    pip install ydb ydb[yc]

Запуск:
    export YDB_ENDPOINT="grpcs://<cluster>.ydb.yandexcloud.net:2135"
    export YDB_DATABASE="/ru-central1/<folder_id>/<db_name>"
    python load_via_ydb_sdk.py --csv ../data/transactions_v2.csv
"""

import csv, ydb, time
from concurrent.futures import ThreadPoolExecutor

ENDPOINT = "grpcs://ydb.serverless.yandexcloud.net:2135"
DATABASE = "/ru-central1/b1ghpijksk2vh66set6s/etnm5jt6vrobqrqbp979"
CSV_PATH = "/home/cloudshell-user/etl-exam/data/transactions_v2.csv"
TOKEN    = "$(yc iam create-token)"
BATCH    = 1000

driver = ydb.Driver(endpoint=ENDPOINT, database=DATABASE,
    credentials=ydb.credentials.AccessTokenCredentials(TOKEN))
driver.wait(fail_fast=True, timeout=10)
pool = ydb.SessionPool(driver, size=10)

def upsert(session, rows):
    vals = ",\n".join(
        f'("{r["call_id"]}","{r["call_time"]}","{r["client_id"]}","{r["region_code"]}",'
        f'"{r["campaign_type"]}","{r["call_status"]}","{r["client_response"]}",'
        f'{int(r["duration_sec"])},"{r["follow_up_required"]}")'
        for r in rows
    )
    session.transaction().execute(
        f"UPSERT INTO transactions_v2 (call_id,call_time,client_id,region_code,"
        f"campaign_type,call_status,client_response,duration_sec,follow_up_required) VALUES {vals};",
        commit_tx=True
    )

def upload_batch(rows):
    pool.retry_operation_sync(upsert, None, rows)

total = 0
batches = []
batch = []
with open(CSV_PATH, newline="") as f:
    for row in csv.DictReader(f):
        batch.append(row)
        if len(batch) >= BATCH:
            batches.append(batch); batch = []
if batch:
    batches.append(batch)

print(f"Всего батчей: {len(batches)}, строк: {sum(len(b) for b in batches):,}")
start = time.time()

with ThreadPoolExecutor(max_workers=8) as ex:
    futures = [ex.submit(upload_batch, b) for b in batches]
    for i, f in enumerate(futures):
        f.result()
        total += len(batches[i])
        print(f"\r  Загружено: {total:,} / {len(batches)*BATCH:,}", end="", flush=True)

elapsed = time.time() - start
print(f"\n✅ Готово! Строк: {total:,} за {elapsed:.0f} сек")
pool.stop(); driver.stop()
