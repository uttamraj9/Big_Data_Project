# Databricks EventHubs Library Fix

## Problem
Installing `com.microsoft.azure:azure-eventhubs-spark_2.13:2.3.22` on cluster `ADBcluster` (ID: `0417-133347-aqen2ief`) fails with:
```
PERMISSION_DENIED: 'com.microsoft.azure:azure-eventhubs-spark_2.13:2.3.22' is not in the artifact allowlist
```

**Root Cause:** The cluster uses Unity Catalog **Shared access mode** (`USER_ISOLATION`), which requires all Maven/JAR libraries to be explicitly added to the metastore allowlist before installation.

---

## Solution 1: Add to Allowlist (Requires Metastore Admin)

### Step 1: Add artifact to Unity Catalog metastore allowlist

You need **metastore admin** privileges. Run this SQL in a Databricks SQL warehouse or notebook:

```sql
-- Add EventHubs Spark connector to allowlist
ALTER METASTORE ADD ARTIFACT 'com.microsoft.azure:azure-eventhubs-spark_2.13:2.3.22' MAVEN;

-- Verify it was added
SHOW ARTIFACTS IN METASTORE;
```

### Step 2: Install the library on the cluster

#### Option A: Via Databricks UI
1. Go to **Compute** → **ADBcluster**
2. Click **Libraries** tab
3. Click **Install new**
4. Select **Maven**
5. Enter coordinates: `com.microsoft.azure:azure-eventhubs-spark_2.13:2.3.22`
6. Click **Install**

#### Option B: Via Azure CLI/REST API
```bash
TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv)
WS="https://adb-7405609294150794.14.azuredatabricks.net"
CLUSTER_ID="0417-133347-aqen2ief"

curl -X POST "https://$WS/api/2.0/libraries/install" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "cluster_id": "'$CLUSTER_ID'",
    "libraries": [
      {
        "maven": {
          "coordinates": "com.microsoft.azure:azure-eventhubs-spark_2.13:2.3.22"
        }
      }
    ]
  }'
```

### Step 3: Verify installation

```bash
curl -s "https://$WS/api/2.0/libraries/cluster-status?cluster_id=$CLUSTER_ID" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

---

## Solution 2: Use Event Hubs Kafka Endpoint (RECOMMENDED)

**Advantages:**
- No library installation required
- Works on Shared clusters without allowlist changes
- Built-in Kafka support in Spark
- `azure-eventhubs-spark` connector is in maintenance mode

### Python/PySpark Code

```python
# Event Hubs connection details
eventhub_namespace = "<your-eventhub-namespace>"  # e.g., "itc-bd-eventhubs"
eventhub_name = "<your-eventhub-name>"
connection_string = "<your-connection-string>"  # From Azure portal

# Extract SAS key from connection string
import re
sas_key_pattern = r"SharedAccessKey=([^;]+)"
sas_key = re.search(sas_key_pattern, connection_string).group(1)

# Kafka configuration
kafka_bootstrap_servers = f"{eventhub_namespace}.servicebus.windows.net:9093"
kafka_sasl = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{connection_string}";'

# Read from Event Hubs using Kafka
df = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
  .option("subscribe", eventhub_name)
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("kafka.sasl.jaas.config", kafka_sasl)
  .option("startingOffsets", "earliest")  # or "latest"
  .load()
)

# Event Hubs message is in the 'value' column (binary)
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

# Define your schema
schema = StructType() \
  .add("transaction_id", StringType()) \
  .add("amount", IntegerType()) \
  # ... add other fields

# Parse JSON from Event Hubs
parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
  .select(from_json("json", schema).alias("data")) \
  .select("data.*")

# Write to Delta Lake
query = (parsed_df.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/mnt/adls/checkpoints/eventhubs")
  .start("/mnt/adls/raw/eventhubs_data")
)
```

### Scala Code (if needed)

```scala
val kafkaBootstrapServers = s"$eventHubNamespace.servicebus.windows.net:9093"
val eventHubName = "<your-eventhub-name>"
val connectionString = "<your-connection-string>"

val df = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBootstrapServers)
  .option("subscribe", eventHubName)
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("kafka.sasl.jaas.config", 
    s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$$ConnectionString" password="$connectionString";""")
  .option("startingOffsets", "earliest")
  .load()
```

---

## Version Notes

- **Cluster Spark Version:** 17.3.x-scala2.13 (DBR 17.3)
- **Scala Version:** 2.13 → Use `azure-eventhubs-spark_2.13`
- **Latest Version:** 2.3.22 (as of 2024)

If using older DBR with Scala 2.12:
- Use `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`

---

## Troubleshooting

### Check cluster security mode:
```bash
TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv)
curl -s "https://adb-7405609294150794.14.azuredatabricks.net/api/2.0/clusters/get?cluster_id=0417-133347-aqen2ief" \
  -H "Authorization: Bearer $TOKEN" | python3 -c "import sys,json; d=json.load(sys.stdin); print('Security Mode:', d.get('data_security_mode'))"
```

**Security Modes:**
- `USER_ISOLATION` (Shared) → Needs allowlist
- `SINGLE_USER` → No allowlist needed
- `NONE` (Legacy) → No allowlist needed

### Check library status:
```bash
curl -s "https://adb-7405609294150794.14.azuredatabricks.net/api/2.0/libraries/cluster-status?cluster_id=0417-133347-aqen2ief" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

### Start the cluster if TERMINATED:
```bash
az databricks cluster start \
  --cluster-id 0417-133347-aqen2ief \
  --resource-group Itc_Bigdata \
  --workspace-name itc-bd-ne-adb
```

---

## Recommendation

**Use Solution 2 (Kafka endpoint)** because:
1. No admin permissions required
2. No library management overhead
3. Kafka support is native to Spark
4. EventHubs-Spark connector is being deprecated
5. Works seamlessly on Shared clusters

Only use Solution 1 if you have legacy code that requires the specific EventHubs connector API.
