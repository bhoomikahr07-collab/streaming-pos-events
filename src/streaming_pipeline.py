"""
The production streaming job: reads POS events from Kafka (or a local
file source for dev), resolves CDC-style changes per micro-batch, and
maintains both a Delta "current state" table and a rolling windowed
sales-metrics table.

The resolve-latest-state and windowing logic implemented here in native
PySpark is the same logic already unit-tested in its plain-Python form
in src/cdc_logic.py — see that module's docstring and README.md
"Why the logic is split this way" for the full explanation.

Usage:
    python src/streaming_pipeline.py --env local          # file source, no Kafka needed
    python src/streaming_pipeline.py --env local-kafka     # reads from a local Kafka broker
    python src/streaming_pipeline.py --env prod             # reads from the production Kafka cluster
"""
import argparse
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from schema import POS_EVENT_SCHEMA
from utils import get_spark_session, load_config


def read_event_stream(spark, cfg, env):
    if env == "local":
        return (
            spark.readStream
            .format("json")
            .schema(POS_EVENT_SCHEMA)
            .option("maxFilesPerTrigger", 1)
            .load(cfg["stream_input_path"])
        )

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka_bootstrap_servers"])
        .option("subscribe", cfg["kafka_topic"])
        .option("startingOffsets", "latest")
        .load()
    )
    return (
        raw
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(F.from_json("json_value", POS_EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )


def resolve_latest_state(batch_df):
    """Keep the highest op_seq per transaction_id."""
    w = Window.partitionBy("transaction_id").orderBy(F.col("op_seq").desc())
    return (
        batch_df
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
    )


def upsert_current_state(spark, resolved_df, current_state_path):
    """MERGE resolved events into the Delta current-state table."""
    from delta.tables import DeltaTable

    non_deletes = resolved_df.filter("op != 'DELETE'")
    deletes = resolved_df.filter("op = 'DELETE'")

    if DeltaTable.isDeltaTable(spark, current_state_path):
        target = DeltaTable.forPath(spark, current_state_path)
        (
            target.alias("t")
            .merge(resolved_df.alias("s"), "t.transaction_id = s.transaction_id")
            .whenMatchedDelete(condition="s.op = 'DELETE'")
            .whenMatchedUpdateAll(condition="s.op != 'DELETE'")
            .whenNotMatchedInsertAll(condition="s.op != 'DELETE'")
            .execute()
        )
    else:
        non_deletes.write.format("delta").save(current_state_path)

    return non_deletes.count(), deletes.count()


def compute_and_write_metrics(spark, current_state_path, metrics_path, window_duration):
    current = spark.read.format("delta").load(current_state_path)
    metrics = (
        current
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
        .groupBy(F.window("event_ts", window_duration), "store_id")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.sum("quantity").alias("total_units"),
            F.count("*").alias("transaction_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "store_id", "total_revenue", "total_units", "transaction_count",
        )
    )
    metrics.write.format("delta").mode("overwrite").save(metrics_path)
    return metrics.count()


def make_batch_processor(spark, cfg):
    def process_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            print(f"[batch {batch_id}] empty, skipping")
            return

        resolved = resolve_latest_state(batch_df)
        upserted, deleted = upsert_current_state(spark, resolved, cfg["current_state_path"])
        window_count = compute_and_write_metrics(
            spark, cfg["current_state_path"], cfg["metrics_path"], cfg["window_duration"]
        )
        print(
            f"[batch {batch_id}] {batch_df.count()} raw events -> "
            f"{upserted} upserted, {deleted} voided -> {window_count} window buckets"
        )

    return process_batch


def main(env: str):
    cfg = load_config(env)
    spark = get_spark_session("streaming-pos-events")
    spark.sparkContext.setLogLevel("WARN")

    events = read_event_stream(spark, cfg, env)

    query = (
        events.writeStream
        .foreachBatch(make_batch_processor(spark, cfg))
        .option("checkpointLocation", cfg["checkpoint_path"])
        .trigger(processingTime=cfg.get("trigger_interval", "10 seconds"))
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=["local", "local-kafka", "prod"], default="local")
    args = parser.parse_args()
    main(args.env)
