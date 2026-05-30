-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Add EventHubs Maven Artifact to Unity Catalog Allowlist
-- MAGIC
-- MAGIC **Run this in Databricks SQL Editor or SQL notebook**
-- MAGIC
-- MAGIC Prerequisites:
-- MAGIC - You must be a **metastore admin**
-- MAGIC - The workspace must have Unity Catalog enabled

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Check current artifacts in allowlist

-- COMMAND ----------

SHOW ARTIFACTS IN METASTORE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Add EventHubs Spark connector to allowlist

-- COMMAND ----------

-- For Scala 2.13 (DBR 17.3.x)
ALTER METASTORE ADD ARTIFACT 'com.microsoft.azure:azure-eventhubs-spark_2.13:2.3.22' MAVEN;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Verify it was added

-- COMMAND ----------

SHOW ARTIFACTS IN METASTORE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Optional: Add for Scala 2.12 (older DBR versions)

-- COMMAND ----------

-- Uncomment if you need Scala 2.12 version
-- ALTER METASTORE ADD ARTIFACT 'com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22' MAVEN;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Next Steps
-- MAGIC
-- MAGIC After running this notebook:
-- MAGIC 1. Go to **Compute** → **ADBcluster**
-- MAGIC 2. Click **Libraries** tab
-- MAGIC 3. Click **Install new** → **Maven**
-- MAGIC 4. Enter: `com.microsoft.azure:azure-eventhubs-spark_2.13:2.3.22`
-- MAGIC 5. Click **Install**
-- MAGIC
-- MAGIC **Alternative (Recommended):** Use Event Hubs Kafka endpoint instead - no library needed!
