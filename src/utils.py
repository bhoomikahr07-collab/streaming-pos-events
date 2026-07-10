"""Shared helpers: config loading and Spark session creation."""
import os
import yaml

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "config.yaml")


def load_config(env: str) -> dict:
    with open(CONFIG_PATH) as f:
        full_config = yaml.safe_load(f)
    if env not in full_config:
        raise ValueError(f"Unknown environment '{env}'. Expected one of: {list(full_config.keys())}")
    return full_config[env]


def get_spark_session(app_name: str):
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
