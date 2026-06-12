"""
Генератор данных transactions_v2 для Задания 1 (YDB → Object Storage).
Создаёт CSV-файл объёмом ~30-35 МБ.
"""

import csv
import random
import uuid
from datetime import datetime, timedelta

REGIONS = [
    "DE-HE", "DE-BY", "DE-NW", "DE-BW", "DE-NI",
    "DE-SN", "DE-RP", "DE-BB", "DE-TH", "DE-ST",
    "DE-MV", "DE-SH", "DE-HB", "DE-SL", "DE-HH",
    "DE-BE"
]
CAMPAIGN_TYPES = [
    "credit_card_offer", "personal_loan", "mortgage_offer",
    "savings_account", "investment_product", "insurance_offer",
    "refinancing_offer", "overdraft_offer"
]
CALL_STATUSES = ["answered", "no_answer", "busy", "voicemail", "callback_requested"]
CLIENT_RESPONSES = [
    "interested", "not_interested", "callback_requested",
    "already_have_product", "will_consider", "transferred_to_agent",
    "no_response", "complaint"
]

def generate_transactions_v2(output_path: str, num_records: int = 300_000):
    """
    Генерирует ~200 000 записей, что даёт приблизительно 30-35 МБ CSV.
    """
    start_date = datetime(2026, 1, 1)
    end_date   = datetime(2026, 5, 31)
    delta_days = (end_date - start_date).days

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "call_id", "call_time", "client_id", "region_code",
            "campaign_type", "call_status", "client_response",
            "duration_sec", "follow_up_required"
        ])

        for i in range(1, num_records + 1):
            call_date = start_date + timedelta(days=random.randint(0, delta_days))
            call_time = call_date + timedelta(
                hours=random.randint(8, 20),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            call_id       = f"call_{call_time.strftime('%Y%m%d')}_{i:06d}"
            client_id     = f"client_{random.randint(1000, 99999)}"
            region_code   = random.choice(REGIONS)
            campaign_type = random.choice(CAMPAIGN_TYPES)
            call_status   = random.choice(CALL_STATUSES)

            if call_status == "answered":
                client_response = random.choice(CLIENT_RESPONSES)
                duration_sec    = random.randint(30, 900)
            else:
                client_response = "no_response"
                duration_sec    = random.randint(5, 30)

            follow_up = random.choice(["true", "false"])

            writer.writerow([
                call_id,
                call_time.strftime("%Y-%m-%d %H:%M:%S"),
                client_id,
                region_code,
                campaign_type,
                call_status,
                client_response,
                duration_sec,
                follow_up
            ])

            if i % 50_000 == 0:
                print(f"  Сгенерировано {i:,} записей...")

    print(f"\n✅ Файл сохранён: {output_path}")


if __name__ == "__main__":
    import os
    os.makedirs("./data", exist_ok=True)
    generate_transactions_v2("./data/transactions_v2.csv", num_records=300_000)
