"""
Simulates a POS event producer. In production this would push to a
Kafka topic or Kinesis stream; locally (and for the pure-Python report
in reports/generate_report.py) it writes newline-delimited JSON to
data/stream_input/, which src/streaming_pipeline.py can also read
directly via Spark's file-based streaming source for local testing
without a broker.

Generates realistic POS traffic: most transactions are a single INSERT,
but a portion later receive a correcting UPDATE (quantity fixed) or a
DELETE (voided), each arriving as a separate event with an incrementing
op_seq — exactly the shape src/cdc_logic.py resolves.

Usage:
    python src/event_producer.py
    python src/event_producer.py --sink kafka --topic pos-events   # requires a running broker
"""
import argparse
import json
import os
import random
from datetime import datetime, timedelta

random.seed(23)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "stream_input")

STORES = [f"S{n:03d}" for n in range(1, 6)]
PRODUCTS = [f"P{n:04d}" for n in range(1, 21)]
PAYMENT_TYPES = ["card", "cash", "mobile_wallet"]

CORRECTION_RATE = 0.06
VOID_RATE = 0.03


def generate_events(start_ts: datetime, duration_minutes: int, events_per_minute: int) -> list[dict]:
    events = []
    event_counter = 1
    txn_counter = 1

    for minute in range(duration_minutes):
        for _ in range(events_per_minute):
            txn_id = f"TXN{txn_counter:07d}"
            txn_counter += 1
            base_ts = start_ts + timedelta(minutes=minute, seconds=random.randint(0, 59))
            store_id = random.choice(STORES)
            product_id = random.choice(PRODUCTS)
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(4.0, 150.0), 2)

            events.append({
                "event_id": f"E{event_counter:07d}",
                "transaction_id": txn_id,
                "op": "INSERT",
                "op_seq": 1,
                "event_ts": base_ts.isoformat(),
                "store_id": store_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "payment_type": random.choice(PAYMENT_TYPES),
            })
            event_counter += 1

            roll = random.random()
            if roll < VOID_RATE:
                void_ts = base_ts + timedelta(seconds=random.randint(30, 180))
                events.append({
                    "event_id": f"E{event_counter:07d}",
                    "transaction_id": txn_id,
                    "op": "DELETE",
                    "op_seq": 2,
                    "event_ts": void_ts.isoformat(),
                    "store_id": store_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "payment_type": "voided",
                })
                event_counter += 1
            elif roll < VOID_RATE + CORRECTION_RATE:
                corrected_qty = max(1, quantity + random.choice([-1, 1, 2]))
                correction_ts = base_ts + timedelta(seconds=random.randint(15, 120))
                events.append({
                    "event_id": f"E{event_counter:07d}",
                    "transaction_id": txn_id,
                    "op": "UPDATE",
                    "op_seq": 2,
                    "event_ts": correction_ts.isoformat(),
                    "store_id": store_id,
                    "product_id": product_id,
                    "quantity": corrected_qty,
                    "unit_price": unit_price,
                    "payment_type": random.choice(PAYMENT_TYPES),
                })
                event_counter += 1

    return events


def write_jsonl(events: list[dict], path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    print(f"wrote {len(events)} events -> {path}")


def write_to_kafka(events: list[dict], topic: str, bootstrap_servers: str = "localhost:9092"):
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    for e in events:
        producer.send(topic, key=e["transaction_id"], value=e)
    producer.flush()
    print(f"published {len(events)} events -> kafka topic '{topic}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sink", choices=["file", "kafka"], default="file")
    parser.add_argument("--topic", default="pos-events")
    parser.add_argument("--duration-minutes", type=int, default=20)
    parser.add_argument("--events-per-minute", type=int, default=15)
    args = parser.parse_args()

    events = generate_events(
        start_ts=datetime(2026, 6, 1, 9, 0, 0),
        duration_minutes=args.duration_minutes,
        events_per_minute=args.events_per_minute,
    )

    if args.sink == "file":
        write_jsonl(events, os.path.join(OUTPUT_DIR, "events_batch_0001.jsonl"))
    else:
        write_to_kafka(events, args.topic)
