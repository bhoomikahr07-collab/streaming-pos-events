"""
generate_report.py

Runs the actual logic in src/cdc_logic.py against generated sample
events and writes a real report — not a mock-up. This is possible
without Spark because the CDC-resolution and windowing logic is plain
Python; only the Kafka-reading and distributed-execution parts of
src/streaming_pipeline.py need an actual Spark/Kafka runtime (which this
sandbox doesn't have — see reports/README.md).

Usage:
    python reports/generate_report.py
"""
import json
import os
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from cdc_logic import resolve_latest_state, compute_windowed_aggregates

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STREAM_INPUT_DIR = os.path.join(BASE_DIR, "data", "stream_input")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
WINDOW_MINUTES = 5


def load_events():
    events = []
    for fname in sorted(os.listdir(STREAM_INPUT_DIR)):
        if fname.endswith(".jsonl"):
            with open(os.path.join(STREAM_INPUT_DIR, fname)) as f:
                events.extend(json.loads(line) for line in f if line.strip())
    return events


def main():
    os.makedirs(REPORTS_DIR, exist_ok=True)
    lines = []

    def log(msg=""):
        print(msg)
        lines.append(msg)

    log("# Rolling Sales Metrics Report (local run)\n")
    log("Runs the real `src/cdc_logic.py` functions against generated sample "
        "events — not a mock-up. See reports/README.md for what this does and doesn't cover.\n")

    events = load_events()
    op_counts = {}
    for e in events:
        op_counts[e["op"]] = op_counts.get(e["op"], 0) + 1

    log("## 1. Raw event stream\n")
    log(f"- total events: {len(events)}")
    for op, count in sorted(op_counts.items()):
        log(f"- {op}: {count}")
    log("")

    resolved = resolve_latest_state(events)
    unique_txns = len({e["transaction_id"] for e in events})
    voided_txns = unique_txns - len(resolved)

    log("## 2. CDC resolution\n")
    log(f"- unique transaction_ids seen: {unique_txns}")
    log(f"- resolved to latest state: {len(resolved)}")
    log(f"- voided (DELETE) transactions excluded: {voided_txns}")
    log(f"- corrections applied (UPDATE events): {op_counts.get('UPDATE', 0)}")
    log("")

    corrected_txn_ids = [e["transaction_id"] for e in events if e["op"] == "UPDATE"]
    if corrected_txn_ids:
        example_id = corrected_txn_ids[0]
        example_events = [e for e in events if e["transaction_id"] == example_id]
        example_resolved = [e for e in resolved if e["transaction_id"] == example_id][0]
        log(f"**Example — transaction `{example_id}`:**\n")
        log("| op | op_seq | quantity | event_ts |")
        log("|---|---|---|---|")
        for e in sorted(example_events, key=lambda x: x["op_seq"]):
            log(f"| {e['op']} | {e['op_seq']} | {e['quantity']} | {e['event_ts']} |")
        log(f"\nResolved state used in aggregation: quantity = **{example_resolved['quantity']}** "
            f"(the correction, not the original) — the original INSERT does not also get counted.\n")

    windowed = compute_windowed_aggregates(resolved, window_minutes=WINDOW_MINUTES)

    log(f"## 3. Rolling sales metrics — {WINDOW_MINUTES}-minute tumbling windows\n")
    log(f"- total windows produced: {len(windowed)}")
    total_revenue = sum(w["total_revenue"] for w in windowed)
    total_units = sum(w["total_units"] for w in windowed)
    log(f"- total revenue across all windows: ${total_revenue:,.2f}")
    log(f"- total units across all windows: {total_units}\n")

    log("**First 10 windows:**\n")
    log("| window_start | store_id | total_revenue | total_units | transaction_count |")
    log("|---|---|---|---|---|")
    for w in windowed[:10]:
        log(f"| {w['window_start']} | {w['store_id']} | ${w['total_revenue']:,.2f} | {w['total_units']} | {w['transaction_count']} |")
    log("")

    top_windows = sorted(windowed, key=lambda w: w["total_revenue"], reverse=True)[:5]
    log("**Top 5 windows by revenue:**\n")
    log("| window_start | store_id | total_revenue |")
    log("|---|---|---|")
    for w in top_windows:
        log(f"| {w['window_start']} | {w['store_id']} | ${w['total_revenue']:,.2f} |")
    log("")

    report_path = os.path.join(REPORTS_DIR, "rolling_metrics_report.md")
    with open(report_path, "w") as f:
        f.write("\n".join(lines))
    print(f"\nReport written to {report_path}")


if __name__ == "__main__":
    main()
