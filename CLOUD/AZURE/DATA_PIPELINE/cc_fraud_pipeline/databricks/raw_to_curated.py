"""
raw_to_curated.py
=================
Databricks PySpark notebook — Incremental Raw → Curated transformation
for cc_fraud_trans using Delta Lake MERGE (upsert).

INCREMENTAL LOAD STRATEGY
--------------------------
ADF writes each day's new records to:
    raw/cc_fraud_trans/ingestion_date=YYYY-MM-DD/cc_fraud_trans.csv

This notebook:
  1. Reads ONLY today's ingestion_date partition from raw
  2. Applies all transformations (identical business rules to ON_PREM)
  3. MERGEs into the Delta curated table on transaction_id
       - Existing rows are updated (late corrections)
       - New rows are inserted
  4. Advances the watermark to max(timestamp) of the batch processed

ON_PREM equivalent flow:
    Hive raw table  →  transformations.py  →  Hive curated table (MERGE)
Azure equivalent flow:
    ADLS raw/cc_fraud_trans/ingestion_date=YYYY-MM-DD/cc_fraud_trans.csv
        →  this notebook (Databricks, Delta MERGE)
            →  ADLS curated/cc_fraud_trans/  (Delta table, partitioned by fraud_label)

Transformations applied (identical to ON_PREM):
    1.  remove_duplicates      — deduplicate on transaction_id
    2.  handle_nulls           — fill / drop nulls per column rules
    3.  extract_time_features  — txn_hour, txn_day_of_week, txn_month, is_weekend
    4.  normalize_amount       — amt_log = log1p(transaction_amount)
    5.  bucket_amount          — amt_bucket: low / medium / high / very_high
    6.  calculate_age          — cardholder_age = card_age (already in months)
    7.  calculate_distance     — distance_km = transaction_distance
    8.  encode_gender          — gender_encoded: biometric auth → 1, others → 0
    9.  transaction_velocity   — txn_velocity_day per user_id per calendar date
    10. flag_high_risk         — high_risk: amount>500 AND hour 0-5 AND velocity>=3

Scheduled via Databricks Job at 02:30 UTC daily (after ADF ingest at 01:00 UTC).
"""

import json
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from delta.tables import DeltaTable

# ─── Widget Parameters (set by Databricks Job) ───────────────
dbutils.widgets.text("adls_account_name", "")
dbutils.widgets.text("raw_container",     "raw")
dbutils.widgets.text("curated_container", "curated")
dbutils.widgets.text("table_name",        "cc_fraud_trans")
dbutils.widgets.text("ingestion_date",    datetime.now(timezone.utc).strftime("%Y-%m-%d"))

ADLS_ACCOUNT   = dbutils.widgets.get("adls_account_name")
RAW_CONTAINER  = dbutils.widgets.get("raw_container")
CUR_CONTAINER  = dbutils.widgets.get("curated_container")
TABLE          = dbutils.widgets.get("table_name")
INGESTION_DATE = dbutils.widgets.get("ingestion_date")

# ─── ADLS Access ─────────────────────────────────────────────
adls_key = dbutils.secrets.get(scope="adls-scope", key="adls-account-key")
spark.conf.set(
    f"fs.azure.account.key.{ADLS_ACCOUNT}.dfs.core.windows.net",
    adls_key
)

# Enable Delta Lake
spark.conf.set("spark.databricks.delta.preview.enabled", "true")
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# ─── Paths ───────────────────────────────────────────────────
BASE              = f"abfss://{{}}@{ADLS_ACCOUNT}.dfs.core.windows.net"
RAW_PARTITION     = f"{BASE.format(RAW_CONTAINER)}/{TABLE}/ingestion_date={INGESTION_DATE}/"
CURATED_PATH      = f"{BASE.format(CUR_CONTAINER)}/{TABLE}/"
WATERMARK_PATH    = f"{BASE.format(RAW_CONTAINER)}/watermark/{TABLE}.json"

print(f"[INFO] Ingestion date  : {INGESTION_DATE}")
print(f"[INFO] Reading raw     : {RAW_PARTITION}")
print(f"[INFO] Delta curated   : {CURATED_PATH}")
print(f"[INFO] Watermark file  : {WATERMARK_PATH}")


# ===========================================================
# Step 1 – Read today's raw partition (incremental slice only)
# ===========================================================
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(RAW_PARTITION)
)
raw_count = df.count()
print(f"[INFO] Raw rows in partition {INGESTION_DATE}: {raw_count}")

if raw_count == 0:
    print("[WARN] No new rows in this partition — nothing to process. Exiting.")
    dbutils.notebook.exit("NO_DATA")

df.printSchema()


# ===========================================================
# Step 2 – remove_duplicates  (ON_PREM: dropDuplicates on transaction_id)
# ===========================================================
df = df.dropDuplicates(["transaction_id"])
print(f"[INFO] After dedup: {df.count()}")


# ===========================================================
# Step 3 – handle_nulls
#   transaction_id / timestamp / user_id / fraud_label  → drop row if null
#   transaction_amount                                  → fill 0.0
#   merchant_category / transaction_type               → fill "unknown"
# ===========================================================
df = df.dropna(subset=["transaction_id", "timestamp", "user_id", "fraud_label"])
df = df.fillna({
    "transaction_amount": 0.0,
    "merchant_category":  "unknown",
    "transaction_type":   "unknown",
})
print(f"[INFO] After null handling: {df.count()}")


# ===========================================================
# Step 4 – extract_time_features
# ===========================================================
df = (
    df
    .withColumn("txn_hour",        F.hour(F.col("timestamp")))
    .withColumn("txn_day_of_week", F.dayofweek(F.col("timestamp")))
    .withColumn("txn_month",       F.month(F.col("timestamp")))
    .withColumn("is_weekend",
        F.when(F.dayofweek(F.col("timestamp")).isin(1, 7), 1).otherwise(0)
    )
)


# ===========================================================
# Step 5 – normalize_amount
# ===========================================================
df = df.withColumn("amt_log", F.log1p(F.col("transaction_amount")))


# ===========================================================
# Step 6 – bucket_amount
# ===========================================================
df = df.withColumn(
    "amt_bucket",
    F.when(F.col("transaction_amount") < 10,  "low")
     .when(F.col("transaction_amount") < 100, "medium")
     .when(F.col("transaction_amount") < 500, "high")
     .otherwise("very_high")
)


# ===========================================================
# Step 7 – calculate_age
# ===========================================================
df = df.withColumn("cardholder_age", F.col("card_age").cast(IntegerType()))


# ===========================================================
# Step 8 – calculate_distance
# ===========================================================
df = df.withColumn("distance_km", F.col("transaction_distance"))


# ===========================================================
# Step 9 – encode_gender
# ===========================================================
BIOMETRIC = ["biometric", "face_recognition", "fingerprint"]
df = df.withColumn(
    "gender_encoded",
    F.when(F.lower(F.col("authentication_method")).isin(BIOMETRIC), 1).otherwise(0)
)


# ===========================================================
# Step 10 – transaction_velocity
# ===========================================================
df = df.withColumn("_txn_date", F.to_date(F.col("timestamp")))
velocity = (
    df.groupBy("user_id", "_txn_date")
      .agg(F.count("*").alias("txn_velocity_day"))
)
df = df.join(velocity, on=["user_id", "_txn_date"], how="left").drop("_txn_date")


# ===========================================================
# Step 11 – flag_high_risk
# ===========================================================
df = df.withColumn(
    "high_risk",
    F.when(
        (F.col("transaction_amount") > 500) &
        (F.col("txn_hour").between(0, 5)) &
        (F.col("txn_velocity_day") >= 3),
        1
    ).otherwise(0)
)


# ===========================================================
# Step 12 – MERGE into Delta curated table (upsert on transaction_id)
#
# ON_PREM equivalent: INSERT ... ON CONFLICT (transaction_id) DO UPDATE SET ...
# Azure equivalent  : DeltaTable.merge() — update existing, insert new
# ===========================================================
curated_count = df.count()
print(f"[INFO] Curated row count (this batch): {curated_count}")
df.show(5, truncate=False)

if DeltaTable.isDeltaTable(spark, CURATED_PATH):
    print("[INFO] Delta table exists — performing MERGE (upsert)")
    delta_table = DeltaTable.forPath(spark, CURATED_PATH)
    (
        delta_table.alias("target")
        .merge(
            df.alias("source"),
            "target.transaction_id = source.transaction_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("[INFO] MERGE complete")
else:
    print("[INFO] Delta table does not exist — performing initial write")
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .partitionBy("fraud_label")
          .save(CURATED_PATH)
    )
    print("[INFO] Initial Delta write complete")


# ===========================================================
# Step 13 – Advance watermark to max(timestamp) of this batch
#
# Only updated for incremental runs. Full load (ingestion_date=full_load)
# must NOT overwrite the watermark — the next daily incremental run should
# still continue from where it left off before the full reload.
# ===========================================================
if INGESTION_DATE == "full_load":
    print("[INFO] Full load run — watermark NOT updated (incremental position preserved)")
else:
    max_ts = df.agg(F.max("timestamp")).collect()[0][0]
    if max_ts is not None:
        new_watermark = json.dumps({"last_watermark": str(max_ts)})
        dbutils.fs.put(WATERMARK_PATH, new_watermark, overwrite=True)
        print(f"[INFO] Watermark advanced to: {max_ts}")
    else:
        print("[WARN] Could not determine max timestamp — watermark NOT updated")

print(f"[INFO] Curated Delta table at: {CURATED_PATH}")
print("[INFO] raw_to_curated (incremental) complete.")
