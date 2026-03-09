#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import random
import uuid
from datetime import datetime, timedelta, UTC

from pymongo import MongoClient


MONGO_HOST = os.getenv("MONGO_HOST", "mongo")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME", "mongo_admin")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "mongo_password")
MONGO_DB = os.getenv("MONGO_INITDB_DATABASE", "etl_mongo")

SESSIONS_PER_BATCH = int(os.getenv("SESSIONS_PER_BATCH", "200"))
EVENTS_PER_BATCH = int(os.getenv("EVENTS_PER_BATCH", "600"))
TICKETS_PER_BATCH = int(os.getenv("TICKETS_PER_BATCH", "120"))

RANDOM_SEED = 42
random.seed(RANDOM_SEED)


DEVICE_TYPES = ["mobile", "desktop", "tablet"]
DEVICE_OS = {
    "mobile": ["Android", "iOS"],
    "desktop": ["Windows", "Linux", "macOS"],
    "tablet": ["Android", "iPadOS"],
}
DEVICE_BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]

PAGES = [
    "/",
    "/catalog",
    "/product/101",
    "/product/102",
    "/product/103",
    "/cart",
    "/checkout",
    "/profile",
    "/support",
    "/favorites",
]

ACTIONS = [
    "login",
    "search",
    "view_product",
    "add_to_cart",
    "remove_from_cart",
    "checkout",
    "logout",
    "open_support_ticket",
]

EVENT_TYPES = [
    "page_view",
    "click",
    "search",
    "add_to_cart",
    "checkout_started",
    "payment_success",
    "payment_failed",
    "ticket_created",
]

ISSUE_TYPES = [
    "payment",
    "delivery",
    "login_issue",
    "refund",
    "account",
]

TICKET_STATUSES = [
    "open",
    "in_progress",
    "resolved",
    "closed",
]


def get_mongo_client() -> MongoClient:
    return MongoClient(
        host=MONGO_HOST,
        port=MONGO_PORT,
        username=MONGO_USER,
        password=MONGO_PASSWORD,
    )


def get_next_batch_number(db) -> int:
    meta = db.etl_meta
    doc = meta.find_one({"_id": "batch_counter"})
    if doc is None:
        meta.insert_one({"_id": "batch_counter", "value": 1})
        return 1

    next_value = int(doc["value"]) + 1
    meta.update_one({"_id": "batch_counter"}, {"$set": {"value": next_value}})
    return next_value


def generate_base_time(batch_number: int) -> datetime:
    base = datetime(2024, 1, 1, 9, 0, 0)
    return base + timedelta(days=(batch_number - 1) * 2)


def random_user_id() -> str:
    return f"user_{random.randint(1, 80):03d}"


def random_device() -> dict:
    device_type = random.choice(DEVICE_TYPES)
    return {
        "type": device_type,
        "os": random.choice(DEVICE_OS[device_type]),
        "browser": random.choice(DEVICE_BROWSERS),
    }


def generate_user_sessions(batch_number: int, base_time: datetime, count: int) -> list[dict]:
    docs = []

    for i in range(count):
        start_dt = base_time + timedelta(
            minutes=random.randint(0, 60 * 24 - 1),
            seconds=random.randint(0, 59),
        )
        duration_sec = random.randint(60, 7200)
        end_dt = start_dt + timedelta(seconds=duration_sec)

        pages_count = random.randint(2, 8)
        actions_count = random.randint(2, 10)

        pages_visited = random.choices(PAGES, k=pages_count)
        actions = random.choices(ACTIONS, k=actions_count)

        doc = {
            "session_id": f"sess_b{batch_number}_{i+1:05d}_{uuid.uuid4().hex[:8]}",
            "user_id": random_user_id(),
            "start_time": start_dt,
            "end_time": end_dt,
            "pages_visited": pages_visited,
            "device": random_device(),
            "actions": actions,
            "batch_no": batch_number,
            "created_at": datetime.now(UTC),
        }
        docs.append(doc)

    return docs


def generate_event_logs(batch_number: int, base_time: datetime, count: int) -> list[dict]:
    docs = []

    for i in range(count):
        event_ts = base_time + timedelta(
            minutes=random.randint(0, 60 * 24 - 1),
            seconds=random.randint(0, 59),
        )
        event_type = random.choice(EVENT_TYPES)

        details = {
            "page": random.choice(PAGES),
            "product_id": str(random.randint(100, 130)),
            "source": random.choice(["banner", "search", "catalog", "recommendation"]),
        }

        doc = {
            "event_id": f"evt_b{batch_number}_{i+1:05d}_{uuid.uuid4().hex[:8]}",
            "timestamp": event_ts,
            "event_type": event_type,
            "details": details,
            "batch_no": batch_number,
            "created_at": datetime.now(UTC),
        }
        docs.append(doc)

    return docs


def generate_ticket_messages(created_at: datetime) -> list[dict]:
    messages = []
    msg_count = random.randint(1, 6)
    current_ts = created_at

    for i in range(msg_count):
        current_ts = current_ts + timedelta(minutes=random.randint(5, 180))
        messages.append(
            {
                "sender": random.choice(["user", "support"]),
                "message_text": f"Message #{i+1} about issue resolution",
                "message_timestamp": current_ts,
            }
        )

    return messages


def generate_support_tickets(batch_number: int, base_time: datetime, count: int) -> list[dict]:
    docs = []

    for i in range(count):
        created_at = base_time + timedelta(
            minutes=random.randint(0, 60 * 24 - 1),
            seconds=random.randint(0, 59),
        )

        status = random.choice(TICKET_STATUSES)
        messages = generate_ticket_messages(created_at)

        if status in {"resolved", "closed"}:
            updated_at = messages[-1]["message_timestamp"] + timedelta(
                minutes=random.randint(10, 300)
            )
        else:
            updated_at = created_at + timedelta(minutes=random.randint(10, 600))

        doc = {
            "ticket_id": f"tkt_b{batch_number}_{i+1:05d}_{uuid.uuid4().hex[:8]}",
            "user_id": random_user_id(),
            "status": status,
            "issue_type": random.choice(ISSUE_TYPES),
            "messages": messages,
            "created_at": created_at,
            "updated_at": updated_at,
            "batch_no": batch_number,
            "created_meta_at": datetime.now(UTC),
        }
        docs.append(doc)

    return docs


def ensure_indexes(db) -> None:
    db.user_sessions.create_index("session_id", unique=True)
    db.user_sessions.create_index("start_time")

    db.event_logs.create_index("event_id", unique=True)
    db.event_logs.create_index("timestamp")

    db.support_tickets.create_index("ticket_id", unique=True)
    db.support_tickets.create_index("updated_at")


def main() -> None:
    client = get_mongo_client()
    db = client[MONGO_DB]

    ensure_indexes(db)

    batch_number = get_next_batch_number(db)
    base_time = generate_base_time(batch_number)

    user_sessions = generate_user_sessions(batch_number, base_time, SESSIONS_PER_BATCH)
    event_logs = generate_event_logs(batch_number, base_time, EVENTS_PER_BATCH)
    support_tickets = generate_support_tickets(batch_number, base_time, TICKETS_PER_BATCH)

    if user_sessions:
        db.user_sessions.insert_many(user_sessions)
    if event_logs:
        db.event_logs.insert_many(event_logs)
    if support_tickets:
        db.support_tickets.insert_many(support_tickets)

    print(f"Inserted batch #{batch_number}")
    print(f"user_sessions:   {len(user_sessions)}")
    print(f"event_logs:      {len(event_logs)}")
    print(f"support_tickets: {len(support_tickets)}")
    print(f"base_time:       {base_time.isoformat()}")


if __name__ == "__main__":
    main()