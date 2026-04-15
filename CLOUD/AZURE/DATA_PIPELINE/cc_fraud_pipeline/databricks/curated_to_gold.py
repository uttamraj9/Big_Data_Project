"""
curated_to_gold.py
==================
Databricks PySpark notebook — Curated → Gold aggregations for cc_fraud_trans.

Reads the curated Parquet layer from ADLS and produces gold-layer aggregation
tables consumed by Synapse Analytics serverless SQL views.

ON_PREM equivalent:
    Hive curated table  →  Hive/HBase gold queries  →  BI / ML feature store
Azure equivalent:
    ADLS curated/cc_fraud_trans/
        →  this notebook (Databricks)
            →  ADLS gold/cc_fraud_trans/
                fraud_by_merchant/      (Parquet)
                fraud_by_device/        (Parquet)
                fraud_by_location/      (Parquet)
                hourly_fraud_pattern/   (Parquet)
                high_risk_summary/      (Parquet)

Synapse gold views (created by IAC/modules/synapse/main.tf) query
these paths via OPENROWSET.

Scheduled via Databricks Job at 03:30 UTC daily (after raw→curated at 02:30).
"""

from pyspark.sql import functions as F

# ─── Widget Parameters ────────────────────────────────────────
dbutils.widgets.text("adls_account_name", "")
dbutils.widgets.text("curated_container", "curated")
dbutils.widgets.text("gold_container",    "gold")
dbutils.widgets.text("table_name",        "cc_fraud_trans")

ADLS_ACCOUNT  = dbutils.widgets.get("adls_account_name")
CUR_CONTAINER = dbutils.widgets.get("curated_container")
GLD_CONTAINER = dbutils.widgets.get("gold_container")
TABLE         = dbutils.widgets.get("table_name")

# ─── ADLS Access ─────────────────────────────────────────────
adls_key = dbutils.secrets.get(scope="adls-scope", key="adls-account-key")
spark.conf.set(
    f"fs.azure.account.key.{ADLS_ACCOUNT}.dfs.core.windows.net",
    adls_key
)

CURATED_PATH = f"abfss://{CUR_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/{TABLE}/"
GOLD_PATH    = f"abfss://{GLD_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/{TABLE}/"

print(f"[INFO] Reading curated: {CURATED_PATH}")
print(f"[INFO] Writing gold:    {GOLD_PATH}")


# ─── Read curated layer ───────────────────────────────────────
df = spark.read.parquet(CURATED_PATH)
print(f"[INFO] Curated row count: {df.count()}")


# ===========================================================
# Gold 1 – Fraud rate by merchant_category
# ===========================================================
fraud_by_merchant = (
    df.groupBy("merchant_category")
      .agg(
          F.count("transaction_id").alias("total_txns"),
          F.sum(F.col("fraud_label").cast("int")).alias("fraud_count"),
          F.round(F.avg(F.col("fraud_label").cast("double")) * 100, 2).alias("fraud_rate_pct"),
          F.round(F.avg("transaction_amount"), 2).alias("avg_txn_amount"),
          F.round(F.sum("transaction_amount"), 2).alias("total_amount"),
      )
      .orderBy(F.desc("fraud_rate_pct"))
)

fraud_by_merchant.show(truncate=False)
fraud_by_merchant.write.format("parquet").mode("overwrite") \
    .save(GOLD_PATH + "fraud_by_merchant/")


# ===========================================================
# Gold 2 – Fraud rate by device_type
# ===========================================================
fraud_by_device = (
    df.groupBy("device_type")
      .agg(
          F.count("transaction_id").alias("total_txns"),
          F.sum(F.col("fraud_label").cast("int")).alias("fraud_count"),
          F.round(F.avg(F.col("fraud_label").cast("double")) * 100, 2).alias("fraud_rate_pct"),
      )
      .orderBy(F.desc("fraud_rate_pct"))
)

fraud_by_device.show(truncate=False)
fraud_by_device.write.format("parquet").mode("overwrite") \
    .save(GOLD_PATH + "fraud_by_device/")


# ===========================================================
# Gold 3 – Fraud rate by location
# ===========================================================
fraud_by_location = (
    df.groupBy("location")
      .agg(
          F.count("transaction_id").alias("total_txns"),
          F.sum(F.col("fraud_label").cast("int")).alias("fraud_count"),
          F.round(F.avg(F.col("fraud_label").cast("double")) * 100, 2).alias("fraud_rate_pct"),
          F.round(F.avg("transaction_amount"), 2).alias("avg_txn_amount"),
      )
      .orderBy(F.desc("fraud_count"))
)

fraud_by_location.show(truncate=False)
fraud_by_location.write.format("parquet").mode("overwrite") \
    .save(GOLD_PATH + "fraud_by_location/")


# ===========================================================
# Gold 4 – Hourly fraud pattern (txn_hour 0-23)
# ===========================================================
hourly_pattern = (
    df.groupBy("txn_hour")
      .agg(
          F.count("transaction_id").alias("total_txns"),
          F.sum(F.col("fraud_label").cast("int")).alias("fraud_count"),
          F.round(F.avg(F.col("fraud_label").cast("double")) * 100, 2).alias("fraud_rate_pct"),
          F.round(F.avg("transaction_amount"), 2).alias("avg_txn_amount"),
      )
      .orderBy("txn_hour")
)

hourly_pattern.show(24, truncate=False)
hourly_pattern.write.format("parquet").mode("overwrite") \
    .save(GOLD_PATH + "hourly_fraud_pattern/")


# ===========================================================
# Gold 5 – High-risk transaction summary by location
# ===========================================================
high_risk_summary = (
    df.filter(F.col("high_risk") == 1)
      .groupBy("location")
      .agg(
          F.count("transaction_id").alias("high_risk_count"),
          F.round(F.sum("transaction_amount"), 2).alias("total_amount"),
          F.round(F.avg("risk_score"), 4).alias("avg_risk_score"),
          F.sum(F.col("fraud_label").cast("int")).alias("confirmed_fraud"),
      )
      .orderBy(F.desc("high_risk_count"))
)

high_risk_summary.show(truncate=False)
high_risk_summary.write.format("parquet").mode("overwrite") \
    .save(GOLD_PATH + "high_risk_summary/")


print(f"[INFO] Gold data written to: {GOLD_PATH}")
print("[INFO] curated_to_gold complete.")
