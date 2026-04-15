# =============================================================
# ITC Big Data Pipeline - Raw → Curated Transformation
# Databricks PySpark Notebook
#
# Flow:
#   ADLS raw/employees/employees.csv
#     → basic transforms
#       → ADLS curated/employees/ (Parquet, partitioned by department)
#         → ADLS gold/employees/  (aggregated Parquet for Synapse)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DecimalType, DateType
import sys

# ─── Widget Parameters (set by Databricks Job) ───────────────
dbutils.widgets.text("adls_account_name", "")
dbutils.widgets.text("raw_container",     "raw")
dbutils.widgets.text("curated_container", "curated")
dbutils.widgets.text("gold_container",    "gold")
dbutils.widgets.text("table_name",        "employees")

ADLS_ACCOUNT  = dbutils.widgets.get("adls_account_name")
RAW_CONTAINER = dbutils.widgets.get("raw_container")
CUR_CONTAINER = dbutils.widgets.get("curated_container")
GLD_CONTAINER = dbutils.widgets.get("gold_container")
TABLE         = dbutils.widgets.get("table_name")

# ─── ADLS Access via Account Key (stored in Databricks secrets)
adls_key = dbutils.secrets.get(scope="adls-scope", key="adls-account-key")

spark.conf.set(
    f"fs.azure.account.key.{ADLS_ACCOUNT}.dfs.core.windows.net",
    adls_key
)

RAW_PATH     = f"abfss://{RAW_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/{TABLE}/"
CURATED_PATH = f"abfss://{CUR_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/{TABLE}/"
GOLD_PATH    = f"abfss://{GLD_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/{TABLE}/"

print(f"[INFO] Reading raw data from: {RAW_PATH}")
print(f"[INFO] Writing curated data to: {CURATED_PATH}")
print(f"[INFO] Writing gold data to: {GOLD_PATH}")

# ─── Step 1: Read from Raw Layer ─────────────────────────────
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(RAW_PATH)

print(f"[INFO] Raw row count: {df_raw.count()}")
df_raw.printSchema()

# ─── Step 2: Basic Transformations ───────────────────────────

# Cast columns to correct types
df_typed = df_raw \
    .withColumn("employee_id", F.col("employee_id").cast(IntegerType())) \
    .withColumn("salary",      F.col("salary").cast(DecimalType(10, 2))) \
    .withColumn("hire_date",   F.col("hire_date").cast(DateType()))

# Clean strings: trim whitespace, lowercase department
df_clean = df_typed \
    .withColumn("first_name",  F.trim(F.col("first_name"))) \
    .withColumn("last_name",   F.trim(F.col("last_name"))) \
    .withColumn("department",  F.trim(F.lower(F.col("department"))))

# Add derived columns
df_enriched = df_clean \
    .withColumn("full_name",     F.concat_ws(" ", F.col("first_name"), F.col("last_name"))) \
    .withColumn("years_service", F.round(F.datediff(F.current_date(), F.col("hire_date")) / 365.25, 1)) \
    .withColumn("salary_band",
        F.when(F.col("salary") < 40000, "Junior")
         .when(F.col("salary") < 70000, "Mid")
         .when(F.col("salary") < 100000, "Senior")
         .otherwise("Executive")) \
    .withColumn("is_active", F.lit(True)) \
    .withColumn("ingestion_date", F.current_date()) \
    .withColumn("ingestion_ts",   F.current_timestamp())

# Remove duplicates on primary key
df_deduped = df_enriched.dropDuplicates(["employee_id"])

# Drop rows where key fields are null
df_final = df_deduped.dropna(subset=["employee_id", "first_name", "last_name", "salary"])

print(f"[INFO] Curated row count after transforms: {df_final.count()}")
df_final.show(10, truncate=False)

# ─── Step 3: Write to Curated Layer (Parquet, partitioned) ───
df_final.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("department") \
    .save(CURATED_PATH)

print(f"[INFO] Curated data written to: {CURATED_PATH}")

# ─── Step 4: Build Gold Aggregations ─────────────────────────

# Gold Table 1: Department summary (for Synapse gold layer)
df_dept_summary = df_final \
    .groupBy("department") \
    .agg(
        F.count("employee_id").alias("headcount"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.min("salary").alias("min_salary"),
        F.max("salary").alias("max_salary"),
        F.min("hire_date").alias("earliest_hire"),
        F.max("hire_date").alias("latest_hire"),
        F.sum(F.when(F.col("is_active"), 1).otherwise(0)).alias("active_count")
    ) \
    .orderBy("department")

df_dept_summary.show()

# Gold Table 2: High earners (salary > 60000 and active)
df_high_earners = df_final \
    .filter((F.col("salary") > 60000) & (F.col("is_active") == True)) \
    .select("employee_id", "full_name", "department", "salary", "salary_band", "years_service")

df_high_earners.show()

# ─── Step 5: Write to Gold Layer ─────────────────────────────
df_dept_summary.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(GOLD_PATH + "dept_summary/")

df_high_earners.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(GOLD_PATH + "high_earners/")

print(f"[INFO] Gold data written to: {GOLD_PATH}")
print("[INFO] Pipeline complete: Raw → Curated → Gold")
