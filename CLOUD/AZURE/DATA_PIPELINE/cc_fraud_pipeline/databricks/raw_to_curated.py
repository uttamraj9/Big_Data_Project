"""
raw_to_curated.py
=================
Databricks PySpark notebook — Raw → Curated transformation for cc_fraud_trans.

Mirrors the ON_PREM curated-layer transformation logic (transformations.py)
with identical business rules, adapted for ADLS Gen2 paths instead of Hive tables.

ON_PREM equivalent flow:
    Hive raw table  →  transformations.py  →  Hive curated table
Azure equivalent flow:
    ADLS raw/cc_fraud_trans/cc_fraud_trans.csv
        →  this notebook (Databricks)
            →  ADLS curated/cc_fraud_trans/  (Parquet, partitioned by fraud_label)

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

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ─── Widget Parameters (set by Databricks Job) ───────────────
dbutils.widgets.text("adls_account_name", "")
dbutils.widgets.text("raw_container",     "raw")
dbutils.widgets.text("curated_container", "curated")
dbutils.widgets.text("table_name",        "cc_fraud_trans")

ADLS_ACCOUNT  = dbutils.widgets.get("adls_account_name")
RAW_CONTAINER = dbutils.widgets.get("raw_container")
CUR_CONTAINER = dbutils.widgets.get("curated_container")
TABLE         = dbutils.widgets.get("table_name")

# ─── ADLS Access ─────────────────────────────────────────────
adls_key = dbutils.secrets.get(scope="adls-scope", key="adls-account-key")
spark.conf.set(
    f"fs.azure.account.key.{ADLS_ACCOUNT}.dfs.core.windows.net",
    adls_key
)

RAW_PATH     = f"abfss://{RAW_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/{TABLE}/"
CURATED_PATH = f"abfss://{CUR_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/{TABLE}/"

print(f"[INFO] Reading raw:    {RAW_PATH}")
print(f"[INFO] Writing curated: {CURATED_PATH}")


# ===========================================================
# Step 1 – Read raw CSV (written by ADF copy activity)
# ===========================================================
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(RAW_PATH)
)
print(f"[INFO] Raw row count: {df.count()}")
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
#   txn_hour, txn_day_of_week (1=Mon…7=Sun), txn_month, is_weekend
# ===========================================================
df = (
    df
    .withColumn("txn_hour",        F.hour(F.col("timestamp")))
    .withColumn("txn_day_of_week", F.dayofweek(F.col("timestamp")))  # 1=Sun,7=Sat in Spark
    .withColumn("txn_month",       F.month(F.col("timestamp")))
    .withColumn("is_weekend",
        F.when(F.dayofweek(F.col("timestamp")).isin(1, 7), 1).otherwise(0)
    )
)


# ===========================================================
# Step 5 – normalize_amount
#   amt_log = log1p(transaction_amount)  (always >= 0 for non-negative amounts)
# ===========================================================
df = df.withColumn("amt_log", F.log1p(F.col("transaction_amount")))


# ===========================================================
# Step 6 – bucket_amount
#   <10 → low, <100 → medium, <500 → high, >=500 → very_high
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
#   card_age column stores age in months; pass through as cardholder_age
# ===========================================================
df = df.withColumn("cardholder_age", F.col("card_age").cast(IntegerType()))


# ===========================================================
# Step 8 – calculate_distance
#   transaction_distance is already in km; rename to distance_km
# ===========================================================
df = df.withColumn("distance_km", F.col("transaction_distance"))


# ===========================================================
# Step 9 – encode_gender
#   biometric authentication methods → 1, all others (incl. null) → 0
#   Column named gender_encoded to match ON_PREM schema
# ===========================================================
BIOMETRIC = ["biometric", "face_recognition", "fingerprint"]
df = df.withColumn(
    "gender_encoded",
    F.when(F.lower(F.col("authentication_method")).isin(BIOMETRIC), 1).otherwise(0)
)


# ===========================================================
# Step 10 – transaction_velocity
#   txn_velocity_day = number of transactions per user_id per calendar date
# ===========================================================
df = df.withColumn("_txn_date", F.to_date(F.col("timestamp")))

velocity = (
    df.groupBy("user_id", "_txn_date")
      .agg(F.count("*").alias("txn_velocity_day"))
)

df = df.join(velocity, on=["user_id", "_txn_date"], how="left") \
       .drop("_txn_date")


# ===========================================================
# Step 11 – flag_high_risk
#   high_risk = 1 if amount > 500 AND txn_hour in [0,5] AND txn_velocity_day >= 3
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
# Step 12 – Write curated Parquet to ADLS, partitioned by fraud_label
# ===========================================================
print(f"[INFO] Curated row count: {df.count()}")
df.show(5, truncate=False)

df.write \
  .format("parquet") \
  .mode("overwrite") \
  .partitionBy("fraud_label") \
  .save(CURATED_PATH)

print(f"[INFO] Curated data written to: {CURATED_PATH}")
print("[INFO] raw_to_curated complete.")
