# Databricks notebook source
# MAGIC %md
# MAGIC # Read Event Hub Data from ADLS Capture (with Auth)
# MAGIC
# MAGIC Event Hubs Capture → ADLS → Databricks (bypasses Kafka connectivity issues)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up ADLS Authentication

# COMMAND ----------

# Using Azure AD passthrough (simplest for interactive use)
# The cluster must have "Azure Data Lake Storage Gen2" credential passthrough enabled
# OR we can use service principal/access key

STORAGE_ACCOUNT = "itcbdneadls"
CONTAINER = "eventhub-capture"

# Mount ADLS if not already mounted
mount_point = "/mnt/eventhub_capture"

try:
    dbutils.fs.ls(mount_point)
    print(f"✅ {mount_point} already mounted")
except:
    print(f"Mounting {CONTAINER}...")

    # Get storage account key
    # Note: In production, use Azure Key Vault or Service Principal
    # For testing, we'll use direct access with SAS or account key

    # Option 1: Set Spark config for this session
    spark.conf.set(
        f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        "SharedKey"
    )

    # Get the account key (you need to set this)
    # ADLS_KEY = dbutils.secrets.get(scope="azure", key="adls-key")
    # For testing, we'll use OAuth/Passthrough

    spark.conf.set(
        f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        "OAuth"
    )
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    )

    # Use cluster's managed identity
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"https://login.microsoftonline.com/2b32b1fa-7899-482e-a6de-be99c0ff5516/oauth2/token"
    )

    print("✅ Auth configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Directly with abfss (no mount needed)

# COMMAND ----------

# Use direct path with OAuth
CAPTURE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/ehubnamespacemay2026/demo-transactions/*/*/*/*/*/*/*.avro"

print(f"Reading from: {CAPTURE_PATH}")

# Try to list files first
try:
    files = dbutils.fs.ls(f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/ehubnamespacemay2026/demo-transactions/")
    print(f"✅ Found {len(files)} partition folders")
    for f in files[:5]:
        print(f"   {f.path}")
except Exception as e:
    print(f"❌ Cannot list files: {e}")
    print("\nTrying alternative: reading from mounted DBFS location...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Use DBFS FileStore

# COMMAND ----------

# Copy one Avro file to DBFS for testing
try:
    # List actual Avro files
    avro_files = dbutils.fs.ls(f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/ehubnamespacemay2026/demo-transactions/0/2026/05/30/23/13/")

    if avro_files:
        first_avro = [f for f in avro_files if f.name.endswith('.avro')][0]
        print(f"Found Avro file: {first_avro.path}")

        # Copy to DBFS
        dbutils.fs.cp(first_avro.path, "/FileStore/eventhub_sample.avro")
        print("✅ Copied to /FileStore/eventhub_sample.avro")

        # Read from DBFS
        df_raw = spark.read.format("avro").load("/FileStore/eventhub_sample.avro")
        print(f"✅ Loaded {df_raw.count()} records")

    else:
        print("❌ No Avro files found")

except Exception as e:
    print(f"❌ Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Transaction Data

# COMMAND ----------

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType

# Transaction schema
transaction_schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("merchant", StringType()) \
    .add("category", StringType()) \
    .add("timestamp", StringType()) \
    .add("location", StringType()) \
    .add("is_fraud", BooleanType()) \
    .add("payment_method", StringType())

try:
    # Parse Body field
    df = (df_raw
        .select(
            col("Body").cast("string").alias("body_string"),
            col("SequenceNumber"),
            col("EnqueuedTimeUtc")
        )
        .select(
            from_json(col("body_string"), transaction_schema).alias("transaction"),
            col("SequenceNumber"),
            col("EnqueuedTimeUtc")
        )
        .select("transaction.*", "SequenceNumber", "EnqueuedTimeUtc")
    )

    print(f"✅ Parsed {df.count()} transactions")
    display(df)

except Exception as e:
    print(f"❌ Error parsing: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ Event Hubs Capture working - data written to ADLS
# MAGIC ⚠️  ADLS auth from Databricks needs configuration
# MAGIC
# MAGIC **Solutions:**
# MAGIC 1. Enable Azure AD Credential Passthrough on cluster
# MAGIC 2. Use Service Principal with RBAC on ADLS
# MAGIC 3. Mount ADLS container with SAS token
# MAGIC 4. Use cluster's managed identity
