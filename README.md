# Near Real-Time Retail Events Pipeline

A near real-time streaming pipeline using PySpark Structured Streaming
to process simulated point-of-sale transaction events, with CDC-style
change handling (corrections and voided transactions) and windowed
aggregations that produce rolling sales metrics for operational
reporting.

## What "CDC-style" means here

POS transactions aren't always final the moment they're rung up — a
cashier corrects a quantity, or a transaction gets voided a minute
later. Each of those shows up as a separate event on the stream:

```json
{"transaction_id": "TXN0001", "op": "INSERT", "op_seq": 1, "quantity": 2, ...}
{"transaction_id": "TXN0001", "op": "UPDATE", "op_seq": 2, "quantity": 3, ...}
```

Rolling sales metrics need to reflect the **latest state** of each
transaction, not double-count every event — that's the "CDC-style
change handling" this pipeline does before aggregating.

## What this project demonstrates

- **PySpark Structured Streaming** reading from Kafka (or a local file
  source for dev/testing) — `src/streaming_pipeline.py`
- **CDC-style change handling** — per micro-batch, resolves each
  `transaction_id` to its latest `op_seq`, drops voided (`DELETE`)
  transactions, and upserts the result into a Delta "current state"
  table via `MERGE` (insert/update/delete all handled)
- **Windowed aggregations** — tumbling-window rolling revenue/units/
  transaction-count per store, recomputed from the resolved state each
  micro-batch, for operational dashboards
- **Reusable, independently-tested domain logic** — the actual
  resolve-latest-state and windowing math live in `src/cdc_logic.py` as
  plain Python functions (no Spark dependency), so they're unit-tested
  directly (`tests/test_cdc_logic.py`) and also power the executed
  sample report in `reports/`

## Repo structure

```
streaming-pos-events/
├── src/
│   ├── schema.py              # POS event schema (Kafka JSON payload)
│   ├── cdc_logic.py           # pure-Python CDC resolution + windowing math (tested + used for reports)
│   ├── event_producer.py      # simulates a Kafka/Kinesis producer -> local JSONL "stream"
│   ├── streaming_pipeline.py  # the real PySpark Structured Streaming job
│   └── utils.py
├── docker/
│   └── docker-compose.yml     # local Kafka + Zookeeper for testing against a real broker
├── config/
│   └── config.yaml
├── reports/
│   ├── generate_report.py     # runs cdc_logic against sample events -> real report
│   ├── rolling_metrics_report.md
│   └── README.md
└── tests/
    └── test_cdc_logic.py      # unit tests for the CDC/windowing logic
```

## Why the logic is split this way

`src/streaming_pipeline.py` is real PySpark Structured Streaming code —
it needs a Spark + Kafka runtime to actually execute, which this sandbox
doesn't have. Rather than leave the core CDC-resolution and
windowing math untestable, that logic is implemented once in
`src/cdc_logic.py` as plain Python (no Spark types), which:

1. Is unit-tested directly (`tests/test_cdc_logic.py`, real, passing).
2. Powers `reports/generate_report.py`, which actually runs against
   generated sample events and produces a genuine, verifiable report
   (`reports/rolling_metrics_report.md`).
3. Documents, in code, exactly what "latest state" and "window bucket"
   mean — which `streaming_pipeline.py`'s Spark implementation
   (window functions + `MERGE` + `groupBy(window(...))`) is built to
   match.

## Running it locally

**Without Kafka** (fastest — uses a local file source):

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

python src/event_producer.py                # writes sample events to data/stream_input/
python reports/generate_report.py            # pure-Python CDC + windowing, real output
pytest tests/                                # or: python -m unittest discover tests -v
```

**With a real local Kafka broker** (closer to production):

```bash
docker compose -f docker/docker-compose.yml up -d
python src/event_producer.py --sink kafka --topic pos-events
python src/streaming_pipeline.py --env local-kafka
```

**On AWS** (Kinesis instead of Kafka): swap the `readStream.format("kafka")`
call in `src/streaming_pipeline.py` for
`readStream.format("kinesis")` with the appropriate connector — the
CDC/windowing logic downstream doesn't change either way, which is the
point of keeping it in `cdc_logic.py`.

## Design notes

- **Why resolve within each micro-batch rather than globally**: Structured
  Streaming's `foreachBatch` gives a clean, testable place to do the
  row-level "latest wins" resolution using an ordinary window function
  (`row_number() over (partition by transaction_id order by op_seq desc)`),
  then `MERGE` that resolved batch into a Delta table that holds the
  running current state — this is the standard Databricks pattern for
  CDC-into-Delta and avoids needing custom stateful operators.
- **Why tumbling windows recomputed from current state, not a running
  sum**: recomputing the aggregate from the current-state table each
  batch (rather than incrementally adding/subtracting) means a late
  correction or void is always reflected correctly, at the cost of
  reprocessing — an acceptable tradeoff at POS-transaction volumes.
