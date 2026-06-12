"""
Генератор JSON-сообщений для Задания 3 (Kafka + PySpark).
Создаёт JSONL-файл объёмом ~20-25 МБ для отправки в топик Kafka.
"""

import json
import random
from datetime import datetime, timedelta


REGIONS = [
    "DE-HE", "DE-BY", "DE-NW", "DE-BW", "DE-NI",
    "DE-SN", "DE-RP", "DE-BB", "DE-TH", "DE-ST",
]
RISK_LEVELS     = ["low", "medium", "high"]
DECISION_STATUS = ["approved", "declined", "manual_review", "pending"]
DOC_TYPES       = ["passport", "driving_license", "residence_permit", "tax_id"]
DOC_STATUSES    = ["verified", "pending_verification", "rejected", "not_required"]


def generate_kafka_messages(output_path: str, num_records: int = 50_000):
    """
    Генерирует ~50 000 JSON-сообщений ≈ 20-25 МБ.
    Каждое сообщение — вложенный JSON (как указано в задании).
    """
    start_date = datetime(2026, 1, 1)
    end_date   = datetime(2026, 5, 31)
    delta_days = (end_date - start_date).days

    with open(output_path, "w", encoding="utf-8") as f:
        for i in range(1, num_records + 1):
            event_date = start_date + timedelta(days=random.randint(0, delta_days))
            event_time = event_date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )

            score = random.randint(300, 850)
            risk  = "low" if score >= 680 else "medium" if score >= 580 else "high"
            amount = random.choice([5000, 7500, 10000, 15000, 20000, 25000, 30000])

            num_docs = random.randint(1, 3)
            documents = [
                {
                    "type":   random.choice(DOC_TYPES),
                    "status": random.choice(DOC_STATUSES)
                }
                for _ in range(num_docs)
            ]

            message = {
                "application_id": f"loan_{random.randint(100000, 999999)}",
                "customer": {
                    "customer_id": f"cust_{random.randint(100, 9999)}",
                    "region":      random.choice(REGIONS)
                },
                "loan": {
                    "amount":      amount,
                    "term_months": random.choice([6, 12, 24, 36, 48, 60])
                },
                "scoring": {
                    "score":      score,
                    "risk_level": risk
                },
                "documents":       documents,
                "decision_status": random.choices(
                    DECISION_STATUS, weights=[55, 25, 15, 5]
                )[0],
                "submitted_at": event_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            }

            f.write(json.dumps(message, ensure_ascii=False) + "\n")

            if i % 10_000 == 0:
                print(f"  Сгенерировано {i:,} сообщений...")

    import os
    size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"\n✅ JSONL-файл сохранён: {output_path}")
    print(f"   Размер: {size_mb:.1f} МБ")
    print(f"   Строк: {num_records:,}")


if __name__ == "__main__":
    import os
    os.makedirs("./data", exist_ok=True)
    generate_kafka_messages("./data/kafka_messages.jsonl", num_records=60_000)
