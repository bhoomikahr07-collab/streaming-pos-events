"""
Core CDC-resolution and windowing logic, deliberately kept as plain
Python (dicts in, dicts out — no Spark/Kafka types) so it can be unit
tested directly and reused by reports/generate_report.py.

src/streaming_pipeline.py implements the same two ideas
(resolve-latest-state and tumbling-window aggregation) using native
PySpark APIs for the real streaming job; the logic here is the
reference spec for what that Spark code needs to do.
"""
from collections import defaultdict
from datetime import datetime, timedelta


def resolve_latest_state(events: list[dict]) -> list[dict]:
    """
    Given a batch of CDC-style events (INSERT/UPDATE/DELETE) for
    possibly-repeated transaction_ids, returns one event per
    transaction_id — its highest op_seq — with voided (DELETE)
    transactions excluded entirely.
    """
    latest: dict[str, dict] = {}
    for event in events:
        key = event["transaction_id"]
        if key not in latest or event["op_seq"] > latest[key]["op_seq"]:
            latest[key] = event
    return [e for e in latest.values() if e["op"] != "DELETE"]


def window_start(event_ts: datetime, window_minutes: int) -> datetime:
    """Floors a timestamp to the start of its tumbling window."""
    epoch = datetime(1970, 1, 1)
    elapsed_seconds = (event_ts - epoch).total_seconds()
    window_seconds = window_minutes * 60
    floored_seconds = (elapsed_seconds // window_seconds) * window_seconds
    return epoch + timedelta(seconds=floored_seconds)


def compute_windowed_aggregates(resolved_events: list[dict], window_minutes: int) -> list[dict]:
    """
    Groups already-CDC-resolved events into tumbling windows per
    (window_start, store_id) and computes rolling sales metrics:
    total revenue, total units, and transaction count.
    """
    buckets = defaultdict(lambda: {"total_revenue": 0.0, "total_units": 0, "transaction_count": 0})

    for event in resolved_events:
        ts = event["event_ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        ws = window_start(ts, window_minutes)
        key = (ws, event["store_id"])

        bucket = buckets[key]
        bucket["total_revenue"] += event["quantity"] * event["unit_price"]
        bucket["total_units"] += event["quantity"]
        bucket["transaction_count"] += 1

    results = []
    for (ws, store_id), agg in buckets.items():
        results.append({
            "window_start": ws.isoformat(),
            "window_end": (ws + timedelta(minutes=window_minutes)).isoformat(),
            "store_id": store_id,
            "total_revenue": round(agg["total_revenue"], 2),
            "total_units": agg["total_units"],
            "transaction_count": agg["transaction_count"],
        })

    return sorted(results, key=lambda r: (r["window_start"], r["store_id"]))
