# Rolling Sales Metrics Report (local run)

Runs the real `src/cdc_logic.py` functions against generated sample events — not a mock-up. See reports/README.md for what this does and doesn't cover.

## 1. Raw event stream

- total events: 332
- DELETE: 13
- INSERT: 300
- UPDATE: 19

## 2. CDC resolution

- unique transaction_ids seen: 300
- resolved to latest state: 287
- voided (DELETE) transactions excluded: 13
- corrections applied (UPDATE events): 19

**Example — transaction `TXN0000019`:**

| op | op_seq | quantity | event_ts |
|---|---|---|---|
| INSERT | 1 | 5 | 2026-06-01T09:01:21 |
| UPDATE | 2 | 7 | 2026-06-01T09:03:11 |

Resolved state used in aggregation: quantity = **7** (the correction, not the original) — the original INSERT does not also get counted.

## 3. Rolling sales metrics — 5-minute tumbling windows

- total windows produced: 21
- total revenue across all windows: $66,261.81
- total units across all windows: 853

**First 10 windows:**

| window_start | store_id | total_revenue | total_units | transaction_count |
|---|---|---|---|---|
| 2026-06-01T09:00:00 | S001 | $2,068.07 | 31 | 12 |
| 2026-06-01T09:00:00 | S002 | $3,543.95 | 45 | 17 |
| 2026-06-01T09:00:00 | S003 | $3,159.13 | 37 | 15 |
| 2026-06-01T09:00:00 | S004 | $3,501.26 | 45 | 15 |
| 2026-06-01T09:00:00 | S005 | $3,773.98 | 36 | 12 |
| 2026-06-01T09:05:00 | S001 | $3,678.74 | 46 | 15 |
| 2026-06-01T09:05:00 | S002 | $2,057.79 | 38 | 16 |
| 2026-06-01T09:05:00 | S003 | $2,715.60 | 30 | 12 |
| 2026-06-01T09:05:00 | S004 | $3,942.21 | 50 | 15 |
| 2026-06-01T09:05:00 | S005 | $3,157.68 | 49 | 15 |

**Top 5 windows by revenue:**

| window_start | store_id | total_revenue |
|---|---|---|
| 2026-06-01T09:10:00 | S003 | $5,461.22 |
| 2026-06-01T09:15:00 | S003 | $4,923.99 |
| 2026-06-01T09:05:00 | S004 | $3,942.21 |
| 2026-06-01T09:00:00 | S005 | $3,773.98 |
| 2026-06-01T09:10:00 | S002 | $3,767.12 |
