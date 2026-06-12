"""
Задание 3: Скрипт для отправки JSON-сообщений в топик Kafka.

Установка:
    pip install kafka-python

Использование:
    python send_to_kafka.py \
        --brokers <broker-host>:9091 \
        --topic   loan-applications \
        --jsonl   ../data/kafka_messages.jsonl \
        --ca-cert /path/to/YandexInternalRootCA.crt \
        --username <service-account-name> \
        --password <service-account-api-key>
"""

import argparse
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError


def create_producer(brokers: str, ca_cert: str, username: str, password: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=brokers.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        security_protocol="SASL_SSL",
        ssl_cafile=ca_cert,
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=username,
        sasl_plain_password=password,
        linger_ms=10,           # небольшая задержка для батчинга
        batch_size=65536,
        acks=1,
        retries=3,
    )


def send_messages(producer: KafkaProducer, topic: str, jsonl_path: str, batch_size: int = 500):
    sent = 0
    errors = 0

    with open(jsonl_path, "r", encoding="utf-8") as f:
        batch = []
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
                batch.append(msg)
            except json.JSONDecodeError as e:
                print(f"⚠️ Ошибка парсинга JSON: {e}")
                errors += 1
                continue

            if len(batch) >= batch_size:
                for m in batch:
                    producer.send(topic, value=m)
                producer.flush()
                sent += len(batch)
                batch = []
                print(f"\r  Отправлено: {sent:,} сообщений", end="", flush=True)

        # Остаток
        if batch:
            for m in batch:
                producer.send(topic, value=m)
            producer.flush()
            sent += len(batch)

    print(f"\n✅ Отправка завершена.")
    print(f"   Отправлено: {sent:,} сообщений")
    print(f"   Ошибок:     {errors:,}")
    return sent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers",  required=True, help="broker:9091")
    parser.add_argument("--topic",    required=True)
    parser.add_argument("--jsonl",    required=True, help="Путь к JSONL-файлу")
    parser.add_argument("--ca-cert",  required=True, help="Путь к CA-сертификату YC")
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    print(f"🔌 Подключение к Kafka: {args.brokers}")
    producer = create_producer(
        args.brokers, args.ca_cert, args.username, args.password
    )

    print(f"📤 Отправка сообщений из {args.jsonl} в топик '{args.topic}'")
    send_messages(producer, args.topic, args.jsonl, args.batch_size)
    producer.close()


if __name__ == "__main__":
    main()
