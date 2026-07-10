"""POS event schema — the JSON payload shape produced by the (simulated)
Kafka/Kinesis producer and consumed by the Structured Streaming job."""
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,
)

POS_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("transaction_id", StringType(), nullable=False),
    StructField("op", StringType(), nullable=False),        # INSERT | UPDATE | DELETE
    StructField("op_seq", IntegerType(), nullable=False),    # per-transaction ordering
    StructField("event_ts", TimestampType(), nullable=False),
    StructField("store_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
    StructField("payment_type", StringType(), nullable=True),
])
