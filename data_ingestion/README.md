# Raw Layer — Data Ingestion (PostgreSQL → HDFS → Hive)

## Overview

This directory implements the **Raw Layer** of the data pipeline — the raw data landing zone. It ingests synthetic credit card fraud transaction data from a local PostgreSQL database into HDFS and makes it queryable via a Hive external table.

> **Training Environment Notice**
> This is a simulation, not a production pipeline. PostgreSQL acts as a stand-in for a real OLTP source system. The raw dataset is a synthetic CSV file pre-split into three subsets to simulate the three ingestion patterns (full load, daily incremental, Kafka streaming) that would exist in a live environment.

---

## Architecture

```
[Synthetic CSV — data/raw/synthetic_fraud_dataset.csv]
        |
        |  simulate_data_split.py  (one-time local setup)
        v
[data/split/]
  full_load.csv          60%  initial historical batch
  incremental_load.csv   20%  simulated daily new records
  kafka_streaming.csv    20%  simulated Kafka stream records
        |
        |  ingest_to_postgres.py  (full / inc / stream)
        v
[PostgreSQL — testdb  (Docker, localhost:5432)]
  cc_fraud_trans            main fraud transaction table
  cc_fraud_streaming_data   streaming simulation table
        |
        |  Sqoop MapReduce job
        v
[HDFS Bronze Landing Zone]
  /tmp/US_UK_05052025/class_project/input/raw_data_sqoop/
        |
        |  Hive DDL  (EXTERNAL TABLE — no data movement)
        v
[Hive — bd_class_project.cc_fraud_trans]
```

---

## Directory Structure

```
data_ingestion/
├── src/
│   └── bronze/
│       ├── simulate_data_split.py        # one-time: splits raw CSV into 3 subsets
│       ├── ingest_to_postgres.py         # loads split CSVs into PostgreSQL
│       ├── create_raw_hive_table.hql  # standalone Hive DDL reference
│       └── oozie/
│           ├── deploy.sh                 # uploads workflows to HDFS, submits jobs
│           ├── full_load/
│           │   ├── workflow.xml          # Oozie: fs -> sqoop -> hive2
│           │   ├── job.properties
│           │   ├── create_raw_hive_table.hql
│           │   └── scripts/raw_full_load.sh
│           └── incremental_load/
│               ├── workflow.xml          # Oozie: shell(watermark) -> sqoop(append)
│               ├── coordinator.xml       # daily schedule at 10:00 AM UTC
│               ├── job.properties
│               └── scripts/
│                   ├── raw_incremental_load.sh
│                   └── get_watermark.sh  # queries MAX(Timestamp) watermark
├── data/
│   ├── raw/
│   │   └── synthetic_fraud_dataset.csv
│   └── split/
│       ├── full_load.csv
│       ├── incremental_load.csv
│       └── kafka_streaming.csv
├── Dockerfile
├── Jenkinsfile
├── requirements.txt
└── README.md
```

---

## Dataset Schema

| Column | Type | Description |
|---|---|---|
| `Transaction_ID` | STRING | Unique transaction identifier |
| `User_ID` | STRING | User account identifier |
| `Transaction_Amount` | DECIMAL(10,2) | Transaction value |
| `Transaction_Type` | STRING | POS / ATM Withdrawal / Bank Transfer / Online |
| `Timestamp` | TIMESTAMP | Transaction date and time |
| `Account_Balance` | DECIMAL(10,2) | Balance at time of transaction |
| `Device_Type` | STRING | Mobile / Tablet / Laptop |
| `Location` | STRING | City of transaction |
| `Merchant_Category` | STRING | Restaurants / Travel / Groceries / Electronics / Clothing |
| `IP_Address_Flag` | INT | 1 = suspicious IP detected |
| `Previous_Fraudulent_Activity` | INT | Prior fraud count for this user |
| `Daily_Transaction_Count` | INT | Transactions today by this user |
| `Avg_Transaction_Amount_7d` | DECIMAL(10,2) | 7-day rolling average amount |
| `Failed_Transaction_Count_7d` | INT | Failed transactions in past 7 days |
| `Card_Type` | STRING | Visa / Mastercard / Amex / Discover |
| `Card_Age` | INT | Card age in months |
| `Transaction_Distance` | DECIMAL(10,2) | Distance (km) from typical location |
| `Authentication_Method` | STRING | PIN / Password / Biometric / OTP |
| `Risk_Score` | DECIMAL(5,4) | Pre-computed fraud risk score (0.0–1.0) |
| `Is_Weekend` | INT | 1 = transaction on weekend |
| `Fraud_Label` | INT | **Target** — 0 = legitimate, 1 = fraudulent |

---

## Cluster Details

| Component | Host |
|---|---|
| Cloudera Manager | `13.41.167.97:7180` |
| HiveServer2 | `ip-172-31-12-74.eu-west-2.compute.internal:10000` |
| Hive Metastore | `ip-172-31-6-42.eu-west-2.compute.internal:9083` |
| HDFS NameNode | `hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020` |
| YARN ResourceManager | `ip-172-31-6-42.eu-west-2.compute.internal:8032` |
| Oozie | `http://ip-172-31-12-74.eu-west-2.compute.internal:11000/oozie` |

---

---

# Part 1 — Manual Execution (Terminal / Bash / Beeline)

Run every step yourself from the terminal. Use this to understand the full pipeline or to debug individual stages.

---

## Step 1 — Start PostgreSQL (local Docker)

```bash
docker run -d \
  --name my-postgres \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin123 \
  -e POSTGRES_DB=testdb \
  -p 5432:5432 \
  postgres:15
```

Verify it is running:

```bash
docker ps | grep my-postgres
docker exec my-postgres psql -U admin -d testdb -c "\dt"
```

---

## Step 2 — Install Python Dependencies

```bash
cd data_ingestion
pip install -r requirements.txt
```

---

## Step 3 — Create the `.env` File

Create `data_ingestion/.env` (this file is gitignored — never commit it):

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=testdb
DB_USERNAME=admin
DB_PASSWORD=admin123
FULL_LOAD_CSV=../data/split/full_load.csv
INCREMENTAL_LOAD_CSV=../data/split/incremental_load.csv
KAFKA_STREAMING_CSV=../data/split/kafka_streaming.csv
LOAD_TABLE=cc_fraud_trans
```

---

## Step 4 — Split the Raw Dataset (one-time)

Splits the raw CSV into three chronological subsets that simulate different ingestion patterns:

```bash
cd data_ingestion
python3 src/raw_layer/simulate_data_split.py
```

Expected output:
```
Simulation data split complete:
  Full Load        (60%): 30,000 rows  -> data/split/full_load.csv
  Incremental Load (20%): 10,000 rows  -> data/split/incremental_load.csv
  Kafka Streaming  (20%): 10,000 rows  -> data/split/kafka_streaming.csv
```

---

## Step 5 — Load Data into PostgreSQL

### Full load (initial setup — replaces the table)

```bash
cd data_ingestion/src/raw_layer
ENV_FILE=../../.env python3 ingest_to_postgres.py full
```

### Verify in PostgreSQL

```bash
docker exec my-postgres psql -U admin -d testdb -c \
  "SELECT COUNT(*) FROM cc_fraud_trans;"

docker exec my-postgres psql -U admin -d testdb -c \
  "SELECT Transaction_ID, Transaction_Amount, Fraud_Label
   FROM cc_fraud_trans LIMIT 5;"
```

### Incremental load (simulate new daily records arriving)

```bash
ENV_FILE=../../.env python3 ingest_to_postgres.py inc
```

### Streaming load (simulate Kafka records)

```bash
ENV_FILE=../../.env python3 ingest_to_postgres.py stream
```

Verify all tables:

```bash
docker exec my-postgres psql -U admin -d testdb -c \
  "SELECT relname, n_live_tup FROM pg_stat_user_tables
   WHERE relname IN ('cc_fraud_trans','cc_fraud_streaming_data');"
```

---

## Step 6 — SSH to the Cloudera Edge Node

All remaining steps (Sqoop, HDFS, Hive) run on the Cloudera cluster edge node:

```bash
ssh -i ~/test_key.pem ec2-user@13.41.167.97
```

---

## Step 7 — Create the HDFS Bronze Landing Directory

```bash
hdfs dfs -mkdir -p /tmp/US_UK_05052025/class_project/input/raw_data_sqoop
sudo -u hdfs hdfs dfs -chmod -R 777 /tmp/US_UK_05052025/class_project/input/raw_data_sqoop

# Confirm directory exists
hdfs dfs -ls /tmp/US_UK_05052025/class_project/input/
```

---

## Step 8 — Full Load: Sqoop Import (PostgreSQL → HDFS)

Run from the edge node. Replace `<source-server-ip>` with the IP of the machine running PostgreSQL (`172.31.8.235` in this training environment):

```bash
sqoop import \
  --connect jdbc:postgresql://172.31.8.235:5432/testdb \
  --username admin \
  --password admin123 \
  --table cc_fraud_trans \
  --delete-target-dir \
  --target-dir /tmp/US_UK_05052025/class_project/input/raw_data_sqoop \
  --m 1 \
  --as-textfile
```

Verify data landed in HDFS:

```bash
hdfs dfs -ls /tmp/US_UK_05052025/class_project/input/raw_data_sqoop/
hdfs dfs -cat /tmp/US_UK_05052025/class_project/input/raw_data_sqoop/part-m-00000 | head -3
```

---

## Step 9 — Create the Hive External Table (Beeline)

Connect to HiveServer2 with beeline:

```bash
beeline -u 'jdbc:hive2://ip-172-31-12-74.eu-west-2.compute.internal:10000/'
```

Inside beeline, run the following DDL:

```sql
-- Create the database
CREATE DATABASE IF NOT EXISTS bd_class_project;

USE bd_class_project;

-- Drop if exists to ensure a clean definition
DROP TABLE IF EXISTS cc_fraud_trans;

-- Create EXTERNAL table pointing to the HDFS directory
-- EXTERNAL means dropping the table will NOT delete the HDFS files
CREATE EXTERNAL TABLE bd_class_project.cc_fraud_trans (
    Transaction_ID               STRING,
    User_ID                      STRING,
    Transaction_Amount           DECIMAL(10,2),
    Transaction_Type             STRING,
    `Timestamp`                  TIMESTAMP,
    Account_Balance              DECIMAL(10,2),
    Device_Type                  STRING,
    Location                     STRING,
    Merchant_Category            STRING,
    IP_Address_Flag              INT,
    Previous_Fraudulent_Activity INT,
    Daily_Transaction_Count      INT,
    Avg_Transaction_Amount_7d    DECIMAL(10,2),
    Failed_Transaction_Count_7d  INT,
    Card_Type                    STRING,
    Card_Age                     INT,
    Transaction_Distance         DECIMAL(10,2),
    Authentication_Method        STRING,
    Risk_Score                   DECIMAL(5,4),
    Is_Weekend                   INT,
    Fraud_Label                  INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/tmp/US_UK_05052025/class_project/input/raw_data_sqoop'
TBLPROPERTIES ("skip.header.line.count"="0");
```

Or run the DDL file directly without entering the interactive shell:

```bash
beeline -u 'jdbc:hive2://ip-172-31-12-74.eu-west-2.compute.internal:10000/' \
  -f ~/bronze_oozie/full_load/create_raw_hive_table.hql
```

---

## Step 10 — Verify the Hive Table (Beeline)

```bash
beeline -u 'jdbc:hive2://ip-172-31-12-74.eu-west-2.compute.internal:10000/bd_class_project' \
  --silent=true \
  -e "SELECT COUNT(*) FROM cc_fraud_trans;"

beeline -u 'jdbc:hive2://ip-172-31-12-74.eu-west-2.compute.internal:10000/bd_class_project' \
  --silent=true \
  -e "SELECT Transaction_ID, Transaction_Type, Transaction_Amount, Fraud_Label
      FROM cc_fraud_trans LIMIT 5;"
```

---

## Step 11 — Incremental Load: Sqoop Append (PostgreSQL → HDFS)

First get the current watermark (max Timestamp) from Hive:

```bash
LAST_VALUE=$(beeline \
  -u 'jdbc:hive2://ip-172-31-12-74.eu-west-2.compute.internal:10000/bd_class_project;' \
  --silent=true --showHeader=false --outputformat=tsv2 \
  -e "SELECT COALESCE(MAX(\`Timestamp\`), '1970-01-01 00:00:00') FROM cc_fraud_trans;" \
  2>/dev/null | tail -1)

echo "Watermark: $LAST_VALUE"
```

Then run the incremental Sqoop import using that watermark:

```bash
sqoop import \
  --connect jdbc:postgresql://172.31.8.235:5432/testdb \
  --username admin \
  --password admin123 \
  --table cc_fraud_trans \
  --incremental append \
  --check-column Timestamp \
  --last-value "${LAST_VALUE}" \
  --target-dir /tmp/US_UK_05052025/class_project/input/raw_data_sqoop \
  --m 1 \
  --as-textfile
```

Verify the new rows are visible in Hive (no DDL change needed — it is an EXTERNAL table):

```bash
beeline -u 'jdbc:hive2://ip-172-31-12-74.eu-west-2.compute.internal:10000/bd_class_project' \
  --silent=true \
  -e "SELECT COUNT(*) FROM cc_fraud_trans;"
```

---

## Step 12 — Run via Oozie (Manual Trigger)

As an alternative to running the individual commands above, you can trigger the Oozie workflows directly from the terminal.

### Full load

```bash
oozie job -run \
  -oozie http://ip-172-31-12-74.eu-west-2.compute.internal:11000/oozie \
  -config ~/bronze_oozie/full_load/job.properties
```

### Incremental load (single run, no coordinator)

```bash
oozie job -run \
  -oozie http://ip-172-31-12-74.eu-west-2.compute.internal:11000/oozie \
  -config ~/bronze_oozie/incremental_load/job.properties \
  -Doozie.coord.application.path= \
  -Doozie.wf.application.path=hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/user/ec2-user/oozie/raw_incremental_load
```

### Check job status

```bash
# Replace <JOB_ID> with the ID printed after submission
oozie job -info <JOB_ID> \
  -oozie http://ip-172-31-12-74.eu-west-2.compute.internal:11000/oozie
```

### Deploy and run everything with the deploy script

```bash
# Upload workflows to HDFS and run full load
bash ~/bronze_oozie/deploy.sh --run-full

# Upload and schedule daily incremental coordinator
bash ~/bronze_oozie/deploy.sh --run-incremental

# Both at once
bash ~/bronze_oozie/deploy.sh --run-full --run-incremental
```

---

---

# Part 2 — Automated Execution (Jenkins)

Jenkins runs the incremental pipeline daily at **10:00 AM** without any manual intervention. The Jenkinsfile drives two stages that mirror the manual steps above.

---

## What Jenkins Does

```
Every day at 10:00 AM
        |
        v
[Stage 1] Simulate New Records in PostgreSQL
        ingest_to_postgres.py inc
        Appends the next batch of rows to cc_fraud_trans in PostgreSQL,
        simulating new daily transactions arriving in the source system.
        |
        v
[Stage 2] Incremental Load: PostgreSQL -> HDFS Bronze
        raw_incremental_load.sh
        1. Reads MAX(Timestamp) from Hive (watermark)
        2. Runs Sqoop --incremental append to push only new rows to HDFS
        3. Hive table picks up the new files automatically (EXTERNAL table)
```

---

## Jenkins Setup (One-Time)

### 1. Add the environment file credential

Jenkins reads all DB connection details from a file credential named `my-env-file`.

In Jenkins → **Manage Jenkins → Credentials → System → Global credentials → Add Credential**:

| Field | Value |
|---|---|
| Kind | Secret file |
| ID | `my-env-file` |
| File | Upload your `.env` file |

The `.env` file must contain:

```env
DB_HOST=172.31.8.235
DB_PORT=5432
DB_NAME=testdb
DB_USERNAME=admin
DB_PASSWORD=admin123
FULL_LOAD_CSV=data_ingestion/data/split/full_load.csv
INCREMENTAL_LOAD_CSV=data_ingestion/data/split/incremental_load.csv
KAFKA_STREAMING_CSV=data_ingestion/data/split/kafka_streaming.csv
LOAD_TABLE=cc_fraud_trans
```

### 2. Create the Jenkins pipeline job

In Jenkins → **New Item → Pipeline**:

- **Name:** `raw-incremental-load`
- **Definition:** Pipeline script from SCM
- **SCM:** Git → your repository URL
- **Script Path:** `data_ingestion/Jenkinsfile`

The `cron('0 10 * * *')` trigger in the Jenkinsfile will schedule it automatically once the job is saved.

---

## Jenkinsfile Stages

```groovy
Stage 1 — Install Dependencies
    Creates (or reuses) a Python virtualenv at:
    /var/lib/jenkins/venvs/raw_ingestion_venv
    Installs packages from requirements.txt

Stage 2 — Simulate New Records in PostgreSQL
    Sources the .env credential file
    Runs: python3 data_ingestion/src/raw_layer/ingest_to_postgres.py inc
    Appends new rows to PostgreSQL, simulating a daily data arrival

Stage 3 — Incremental Load: PostgreSQL -> HDFS Bronze
    Sources the .env credential file
    Runs: data_ingestion/src/raw_layer/oozie/incremental_load/scripts/raw_incremental_load.sh
    1. Reads MAX(Timestamp) from Hive via beeline
    2. Runs Sqoop incremental append to push new rows to HDFS
```

---

## Trigger a Manual Build in Jenkins

To run the pipeline immediately without waiting for the scheduled time:

1. Open Jenkins → **raw-incremental-load** job
2. Click **Build Now**
3. Open the build → **Console Output** to watch the live logs

---

## Monitor the Pipeline

### Check the last build result

```
Jenkins UI → raw-incremental-load → Build History → latest build → Console Output
```

### Useful console output to look for

```
# Stage 2 — successful incremental load to PostgreSQL
Incremental load complete: 1000 new rows appended to 'cc_fraud_trans'.

# Stage 3 — Sqoop confirmed rows imported
Retrieved 1000 records.
Sqoop Incremental Import Successful
```

### Verify after a Jenkins run (on the edge node)

```bash
# Check how many rows are now in Hive
beeline -u 'jdbc:hive2://ip-172-31-12-74.eu-west-2.compute.internal:10000/bd_class_project' \
  --silent=true \
  -e "SELECT COUNT(*) FROM cc_fraud_trans;"

# Check the latest records landed in HDFS
hdfs dfs -ls -t /tmp/US_UK_05052025/class_project/input/raw_data_sqoop/ | head -5
```

---

## Notes

- The `.env` file is gitignored — never commit credentials.
- The Hive table is **EXTERNAL**: dropping it does not delete HDFS files.
- Sqoop uses `--m 1` (single mapper) — fine for this small training dataset.
- The incremental load uses a **timestamp watermark**: only rows newer than `MAX(Timestamp)` in Hive are imported on each run.
- For a full reset, re-run Part 1 from Step 5 onwards, or trigger the full load Oozie workflow.
