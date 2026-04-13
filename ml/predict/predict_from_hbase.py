#!/usr/bin/env python3
"""
predict_from_hbase.py
=====================
Real-time Fraud Detection — Prediction Cron Job

Reads new transactions from HBase (cc_fraud_realtime), applies the trained
RandomForest model, and appends predictions to Hive (bd_class_project.predictions_realtime).

Architecture
------------
  Kafka producer  ─→  Kafka: cc_fraud_stream
                              |
                              v
                      Kafka consumer  ─→  HBase: cc_fraud_realtime
                                                      |
                                    (this script) ────┘
                                          |
                                          v  feature engineering
                                    PipelineModel (VectorAssembler + RF)
                                          |
                                          v
                              Hive: bd_class_project.predictions_realtime
"""

import os
import sys
import logging

import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_replace, to_timestamp, hour, dayofweek, dayofmonth,
    when, lower, col, max as spark_max,
)
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import PipelineModel

# ── Configuration ─────────────────────────────────────────────────────────────
HBASE_HOST        = os.environ.get("HBASE_HOST",          "172.31.6.42")
HBASE_PORT        = int(os.environ.get("HBASE_PORT",       "9090"))
HBASE_TABLE       = os.environ.get("HBASE_TABLE",          "cc_fraud_realtime")
HIVE_URI          = os.environ.get("HIVE_METASTORE_URIS",  "thrift://172.31.6.42:9083")
MODEL_PATH        = os.environ.get("MODEL_PATH",           "file:///app/model")
PREDICTIONS_DB    = os.environ.get("PREDICTIONS_DB",       "bd_class_project")
PREDICTIONS_TABLE = os.environ.get("PREDICTIONS_TABLE",    "predictions_realtime")
CF                = "cf"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Column list (must match what the Kafka consumer stores in HBase) ──────────
# Row key Transaction_ID is added separately; all others come from cf:* columns.
HBASE_COLUMNS = [
    "user_id", "transaction_amount", "transaction_type", "timestamp",
    "account_balance", "device_type", "location", "merchant_category",
    "ip_address_flag", "previous_fraudulent_activity", "daily_transaction_count",
    "avg_transaction_amount_7d", "failed_transaction_count_7d",
    "card_type", "card_age", "transaction_distance",
    "authentication_method", "risk_score", "is_weekend",
]

# Numeric columns that need casting (HBase stores all values as strings)
NUMERIC_COLS = [
    "transaction_amount", "account_balance", "card_age", "transaction_distance",
    "risk_score", "ip_address_flag", "previous_fraudulent_activity",
    "daily_transaction_count", "avg_transaction_amount_7d",
    "failed_transaction_count_7d", "is_weekend",
]

# Categorical columns and expected values — must exactly match training
CATEGORIES = {
    "transaction_type":      ["pos", "bank_transfer", "online", "atm_withdrawal"],
    "device_type":           ["mobile", "tablet", "laptop"],
    "location":              ["tokyo", "mumbai", "london", "sydney", "new_york"],
    "merchant_category":     ["restaurants", "clothing", "travel", "groceries", "electronics"],
    "card_type":             ["mastercard", "amex", "discover", "visa"],
    "authentication_method": ["pin", "password", "biometric", "otp"],
}

RAW_SCHEMA = StructType(
    [StructField("transaction_id", StringType(), True)]
    + [StructField(c, StringType(), True) for c in HBASE_COLUMNS]
)


# ── HBase helpers ─────────────────────────────────────────────────────────────

def get_watermark(spark: SparkSession) -> str:
    """Return the latest timestamp already in the predictions table (watermark)."""
    full_table = f"{PREDICTIONS_DB}.{PREDICTIONS_TABLE}"
    try:
        tbl = spark.table(full_table)
        if tbl.rdd.isEmpty():
            return "1970-01-01 00:00:00"
        max_ts = (
            tbl.select(spark_max(to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")))
               .first()[0]
        )
        return max_ts.strftime("%Y-%m-%d %H:%M:%S") if max_ts else "1970-01-01 00:00:00"
    except Exception as exc:
        log.warning("Could not read watermark from %s (%s) — starting from epoch.", full_table, exc)
        return "1970-01-01 00:00:00"


def scan_hbase(last_time: str) -> list:
    """
    Scan HBase cc_fraud_realtime for rows with Timestamp > last_time.

    Uses a server-side SingleColumnValueFilter so only matching rows are
    transferred over the Thrift connection.

    Returns a list of dicts with lowercase column names.
    """
    log.info("Connecting to HBase Thrift at %s:%d", HBASE_HOST, HBASE_PORT)
    conn = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, timeout=15000)
    try:
        table = conn.table(HBASE_TABLE)

        # Server-side filter: only rows where cf:Timestamp > last_time
        # 'binary:' comparator does lexicographic byte comparison —
        # works correctly for ISO-format timestamps ("YYYY-MM-DD HH:MM:SS")
        scan_filter = (
            f"SingleColumnValueFilter('{CF}', 'Timestamp', >, "
            f"'binary:{last_time}', true, true)"
        )
        log.info("HBase scan filter: cf:Timestamp > '%s'", last_time)

        rows = []
        for row_key, data in table.scan(filter=scan_filter):
            # row_key  → transaction_id
            record = {"transaction_id": row_key.decode()}
            for col_bytes, val_bytes in data.items():
                # col_bytes = b'cf:User_ID'  →  strip CF prefix, lowercase
                col_name = col_bytes.decode().split(":", 1)[1].lower()
                record[col_name] = val_bytes.decode()
            rows.append(record)

        log.info("HBase scan returned %d new rows", len(rows))
        return rows
    finally:
        conn.close()


# ── Feature engineering ───────────────────────────────────────────────────────

def engineer_features(raw_df):
    """
    Apply the same transformations used during model training so the feature
    vector matches what the saved RandomForest expects.
    """
    df = raw_df

    # 1. Cast numeric columns (HBase stores everything as strings)
    for c in NUMERIC_COLS:
        df = df.withColumn(c, col(c).cast("double"))

    # 2. user_id: strip 'USER_' prefix, cast to int
    df = df.withColumn("user_id", regexp_replace("user_id", "(?i)^USER_", "").cast("int"))

    # 3. Timestamp features
    df = (df
          .withColumn("ts",         to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
          .withColumn("hour",       hour("ts"))
          .withColumn("dayofweek",  dayofweek("ts"))
          .withColumn("dayofmonth", dayofmonth("ts")))

    # 4. Manual OHE binary columns for each categorical feature
    #    regexp_replace normalises "Bank Transfer" → "bank_transfer", "New York" → "new_york"
    for cat_col, vals in CATEGORIES.items():
        clean = regexp_replace(lower(col(cat_col)), "[\\s-]+", "_")
        df = df.withColumn(cat_col, clean)
        for v in vals:
            df = df.withColumn(f"{cat_col}_{v}", when(col(cat_col) == v, 1).otherwise(0))

    # 5. Drop columns not needed by the model
    drop_cols = list(CATEGORIES.keys()) + ["timestamp", "ts"]
    df = df.drop(*drop_cols)

    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    spark = (
        SparkSession.builder
          .appName("predict-from-hbase")
          .config("hive.metastore.uris", HIVE_URI)
          .enableHiveSupport()
          .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1. Watermark — last timestamp already scored
    last_time = get_watermark(spark)
    log.info("Watermark: will process rows where Timestamp > '%s'", last_time)

    # 2. Read new records from HBase
    raw_rows = scan_hbase(last_time)
    if not raw_rows:
        log.info("No new records in HBase since '%s' — nothing to do.", last_time)
        spark.stop()
        sys.exit(0)

    # 3. Build Spark DataFrame
    #    Only keep columns defined in RAW_SCHEMA; ignore any extra HBase columns
    schema_cols = {f.name for f in RAW_SCHEMA.fields}
    clean_rows  = [{k: v for k, v in r.items() if k in schema_cols} for r in raw_rows]
    raw_df = spark.createDataFrame(clean_rows, schema=RAW_SCHEMA)
    log.info("Created DataFrame with %d rows and %d columns", raw_df.count(), len(raw_df.columns))

    # 4. Feature engineering
    features_df = engineer_features(raw_df)

    # 5. Load model and score
    log.info("Loading PipelineModel from %s", MODEL_PATH)
    pipeline = PipelineModel.load(MODEL_PATH)
    scored   = pipeline.transform(features_df)

    # 6. Join predictions back onto the raw fields for a rich output row
    preds_df = scored.select("transaction_id", "prediction")
    out_df   = raw_df.join(preds_df, on="transaction_id", how="inner")

    count = out_df.count()
    log.info("Scored %d records. Preview:", count)
    out_df.select(
        "transaction_id", "user_id", "timestamp", "transaction_amount", "prediction"
    ).show(10, truncate=False)

    # 7. Persist to Hive (create table on first run, append thereafter)
    full_out = f"{PREDICTIONS_DB}.{PREDICTIONS_TABLE}"
    (out_df.write
           .mode("append")
           .format("hive")
           .saveAsTable(full_out))
    log.info("Wrote %d predictions to Hive table '%s'", count, full_out)

    spark.stop()


if __name__ == "__main__":
    main()
