"""
Генератор данных для Задания 2 (Airflow + DataProc).
Создаёт Parquet-файл объёмом ~50-60 МБ.
"""

import random
import uuid
from datetime import datetime, timedelta

try:
    import pandas as pd
    PANDAS_OK = True
except ImportError:
    PANDAS_OK = False


REGIONS = [
    "DE-HE", "DE-BY", "DE-NW", "DE-BW", "DE-NI",
    "DE-SN", "DE-RP", "DE-BB", "DE-TH", "DE-ST",
    "DE-MV", "DE-SH", "DE-HB", "DE-SL", "DE-HH",
    "DE-BE"
]
PRODUCT_TYPES   = ["cash_loan", "car_loan", "mortgage", "credit_line", "overdraft", "student_loan"]
RISK_LEVELS     = ["low", "medium", "high"]
DECISION_STATUS = ["approved", "declined", "manual_review", "pending"]
CHANNELS        = ["mobile", "web", "branch", "call_center", "partner"]


def generate_loan_applications(output_path: str, num_records: int = 350_000):
    """
    Генерирует ~350 000 записей ≈ 50-60 МБ Parquet.
    """
    start_date = datetime(2026, 1, 1)
    end_date   = datetime(2026, 5, 31)
    delta_days = (end_date - start_date).days

    rows = []
    for i in range(1, num_records + 1):
        event_date = start_date + timedelta(days=random.randint(0, delta_days))
        event_time = event_date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        app_id           = f"app_{event_time.strftime('%Y%m%d')}_{i:06d}"
        customer_id      = f"cust_{random.randint(10000, 999999)}"
        region_code      = random.choice(REGIONS)
        product_type     = random.choice(PRODUCT_TYPES)
        requested_amount = random.choice([
            5000, 7500, 10000, 12000, 15000, 20000, 25000, 30000, 50000
        ])
        term_months      = random.choice([6, 12, 24, 36, 48, 60])
        credit_score     = random.randint(300, 850)
        risk_level       = (
            "low" if credit_score >= 680
            else "medium" if credit_score >= 580
            else "high"
        )
        decision         = random.choices(
            DECISION_STATUS,
            weights=[55, 25, 15, 5]
        )[0]
        approved_amount  = requested_amount if decision == "approved" else 0
        channel          = random.choice(CHANNELS)
        employee_review  = random.choice(["true", "false"])
        processing_time  = random.randint(5, 300)

        rows.append({
            "application_id":       app_id,
            "event_time":           event_time.strftime("%Y-%m-%d %H:%M:%S"),
            "customer_id":          customer_id,
            "region_code":          region_code,
            "product_type":         product_type,
            "requested_amount":     requested_amount,
            "term_months":          term_months,
            "credit_score":         credit_score,
            "risk_level":           risk_level,
            "decision_status":      decision,
            "approved_amount":      approved_amount,
            "channel":              channel,
            "employee_review_flag": employee_review,
            "processing_time_sec":  processing_time,
        })

        if i % 100_000 == 0:
            print(f"  Сгенерировано {i:,} записей...")

    if PANDAS_OK:
        df = pd.DataFrame(rows)
        df.to_csv(output_path, index=False)
        print(f"\n✅ CSV-файл сохранён: {output_path}")
        print(f"   Размер: {__import__('os').path.getsize(output_path) / 1024 / 1024:.1f} МБ")
        print(f"   Строк: {len(df):,}")
    else:
        import csv
        csv_path = output_path.replace(".parquet", ".csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"\n✅ CSV-файл сохранён: {csv_path} (pandas не найден, сохранено как CSV)")


if __name__ == "__main__":
    import os
    os.makedirs("./data", exist_ok=True)
    generate_loan_applications("./data/loan_applications.csv", num_records=500_000)
