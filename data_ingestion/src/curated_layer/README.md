# Curated Layer — cc_fraud_trans

Transforms raw credit card fraud data from the **Hive raw layer** into an enriched
**Hive curated layer** using PySpark.  Includes 10 business transformations, unit
tests (pytest + mock data), and data quality validation (Great Expectations).

```
Source : bd_class_project.cc_fraud_trans         (Hive raw)
Target : bd_class_project.cc_fraud_trans_curated  (Hive curated)
```

## Architecture

```
Hive Raw Layer (cc_fraud_trans)
        |
        |  spark-submit full_load.py / incremental_load.py
        v
  10 Transformations (transformations.py)
        |
        v
Hive Curated Layer (cc_fraud_trans_curated)
        |
        v
Great Expectations — data quality gate
```

## 10 Transformations

| # | Function | New Column(s) | Description |
|---|----------|---------------|-------------|
| 1 | `remove_duplicates` | — | Drop duplicate rows on `trans_num` |
| 2 | `handle_nulls` | — | Fill `amt=0`, `category/merchant/gender="unknown"`; drop rows missing key cols |
| 3 | `extract_time_features` | `txn_hour`, `txn_day_of_week`, `txn_month`, `is_weekend` | Calendar and time-of-day features from `Timestamp` |
| 4 | `normalize_amount` | `amt_log` | `log1p(amt)` to compress skewed distribution |
| 5 | `bucket_amount` | `amt_bucket` | `low / medium / high / very_high` tiers |
| 6 | `calculate_age` | `cardholder_age` | Age in years from `dob` vs transaction date |
| 7 | `calculate_distance` | `distance_km` | Haversine distance between cardholder and merchant |
| 8 | `encode_gender` | `gender_encoded` | `F→0`, `M→1`, other→`-1` |
| 9 | `transaction_velocity` | `txn_velocity_day` | Transactions per card per calendar day (window function) |
| 10 | `flag_high_risk` | `high_risk` | `1` if `amt>500` AND `hour 0–5` AND `velocity>3` |

---

## Part 1 — Manual Execution (Terminal / Bash / Beeline)

Run all steps from the **Cloudera edge node** (`ec2-user@13.41.167.97`) unless stated otherwise.

### Step 1 — SSH to the edge node

```bash
ssh -i ~/test_key.pem ec2-user@13.41.167.97
```

### Step 2 — Clone / pull the repo

```bash
# First time
git clone https://github.com/uttamraj9/Big_Data_Project.git ~/Big_Data_Project

# Subsequent runs — pull latest
cd ~/Big_Data_Project
git pull origin main
```

### Step 3 — Verify the raw Hive table exists

The raw table must be populated before running the curated layer.

```bash
beeline -u "jdbc:hive2://ip-172-31-12-74.eu-west-2.compute.internal:10000/bd_class_project;" \
  --silent=true \
  -e "SELECT COUNT(*) FROM cc_fraud_trans;"
```

Expected output: at least one row (e.g. `31000`).

### Step 4 — Run unit tests locally (optional but recommended)

Unit tests use a local PySpark session with mock data — no cluster required.

```bash
# On the edge node or any machine with Python 3 + pip
cd ~/Big_Data_Project

pip install --quiet \
  pyspark==3.3.0 \
  pytest==7.4.0 \
  pytest-cov==4.1.0 \
  great-expectations==0.17.23 \
  pandas==1.5.3 \
  pyarrow==12.0.0

pytest data_ingestion/src/curated_layer/tests/ -v --tb=short
```

All 11 test classes should pass (one per transformation + `TestApplyAll`).

### Step 5 — Full Load (first run or full refresh)

Reads the entire raw table, applies all 10 transformations, and **overwrites**
the curated table.

```bash
cd ~/Big_Data_Project/data_ingestion/src/curated_layer

bash scripts/curated_full_load.sh
```

What the script does internally:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --name curated-full-load \
  --conf spark.sql.hive.convertMetastoreParquet=false \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --py-files spark/transformations.py \
  spark/full_load.py
```

Or run the Spark job directly:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --py-files spark/transformations.py \
  spark/full_load.py
```

### Step 6 — Verify the curated table

```bash
beeline -u "jdbc:hive2://ip-172-31-12-74.eu-west-2.compute.internal:10000/bd_class_project;" \
  --silent=true \
  -e "
SELECT COUNT(*)                       AS total_rows,
       COUNT(DISTINCT trans_num)      AS unique_txns,
       SUM(is_fraud)                  AS fraud_count,
       SUM(high_risk)                 AS high_risk_count,
       MIN(cardholder_age)            AS min_age,
       MAX(cardholder_age)            AS max_age
FROM cc_fraud_trans_curated;
"
```

Check a sample of the new columns:

```bash
beeline -u "jdbc:hive2://ip-172-31-12-74.eu-west-2.compute.internal:10000/bd_class_project;" \
  --silent=true \
  -e "
SELECT trans_num, amt, amt_log, amt_bucket,
       txn_hour, is_weekend, cardholder_age,
       ROUND(distance_km, 2) AS dist_km,
       txn_velocity_day, high_risk
FROM cc_fraud_trans_curated
LIMIT 10;
"
```

### Step 7 — Incremental Load (daily delta)

Reads only rows from the raw table with `Timestamp` newer than the current
maximum in the curated table, applies all transformations, and **appends**.

```bash
cd ~/Big_Data_Project/data_ingestion/src/curated_layer

bash scripts/curated_incremental_load.sh
```

Or directly:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --py-files spark/transformations.py \
  spark/incremental_load.py
```

Expected output:

```
[1/4] Getting watermark from curated table...
      Watermark: 2023-09-27 19:28:00
[2/4] Reading new rows from bd_class_project.cc_fraud_trans where Timestamp > '2023-09-27 19:28:00'
      New rows: 1000
[3/4] Applying transformations...
[4/4] Appending to curated table: bd_class_project.cc_fraud_trans_curated
Incremental load complete. Rows appended: 1000
```

If no new rows exist:

```
No new records. Curated table is up to date.
```

### Step 8 — Data Quality Check (Great Expectations)

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --name curated-data-quality \
  --conf spark.sql.hive.convertMetastoreParquet=false \
  data_ingestion/src/curated_layer/data_quality/great_expectations_suite.py
```

The suite validates 14 expectation categories including:

- No nulls in `trans_num`, `Timestamp`, `cc_num`, `amt`, `is_fraud`
- `amt >= 0`, `amt_log >= 0`, `txn_hour` in 0–23, `txn_month` in 1–12
- `cardholder_age` between 18–120
- `distance_km >= 0`, `txn_velocity_day >= 1`
- `amt_bucket` in `{low, medium, high, very_high}`
- `is_fraud` mean between 0.0–0.5 (not all rows are fraud)
- `trans_num` is unique

Expected output on success:

```
Data Quality Results:
  Evaluated:  14
  Successful: 14
  Failed:     0
  Success:    True
```

### Step 9 — Run via Oozie (scheduled)

Deploy the Oozie workflow files to HDFS and submit:

```bash
cd ~/Big_Data_Project/data_ingestion/src/curated_layer

# Upload workflow files to HDFS
OOZIE_BASE="hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/user/ec2-user/oozie"

# Full load
hdfs dfs -mkdir -p "${OOZIE_BASE}/curated_full_load"
hdfs dfs -put -f oozie/full_load/workflow.xml   "${OOZIE_BASE}/curated_full_load/"
hdfs dfs -put -f oozie/full_load/job.properties "${OOZIE_BASE}/curated_full_load/"
hdfs dfs -put -f spark/transformations.py       "${OOZIE_BASE}/curated_full_load/"
hdfs dfs -put -f spark/full_load.py             "${OOZIE_BASE}/curated_full_load/"

# Submit full load
oozie job -run \
  -oozie http://ip-172-31-12-74.eu-west-2.compute.internal:11000/oozie \
  -config oozie/full_load/job.properties

# Incremental load
hdfs dfs -mkdir -p "${OOZIE_BASE}/curated_incremental_load"
hdfs dfs -put -f oozie/incremental_load/workflow.xml   "${OOZIE_BASE}/curated_incremental_load/"
hdfs dfs -put -f oozie/incremental_load/job.properties "${OOZIE_BASE}/curated_incremental_load/"
hdfs dfs -put -f spark/transformations.py              "${OOZIE_BASE}/curated_incremental_load/"
hdfs dfs -put -f spark/incremental_load.py             "${OOZIE_BASE}/curated_incremental_load/"

oozie job -run \
  -oozie http://ip-172-31-12-74.eu-west-2.compute.internal:11000/oozie \
  -config oozie/incremental_load/job.properties
```

Monitor the job:

```bash
oozie job -info <JOB_ID> \
  -oozie http://ip-172-31-12-74.eu-west-2.compute.internal:11000/oozie
```

---

## Part 2 — Automated Execution (Jenkins)

Jenkins pipeline: **`curated-layer-ingestion`**
URL: `http://13.42.152.118:8080/job/curated-layer-ingestion/`
Jenkinsfile: `data_ingestion/src/curated_layer/Jenkinsfile`

### One-time Jenkins Setup

#### 1. Credentials required

| Credential ID | Type | Value |
|---------------|------|-------|
| `github-token` | Username + token | GitHub token for `uttamraj9` (already set) |
| `my-env-file` | Secret file | `.env` with `DB_HOST=13.42.152.118`, `DB_PORT=5432`, `DB_NAME=testdb`, `DB_USERNAME=admin`, `DB_PASSWORD=admin123` (already set) |

SSH key for edge node is stored at `/var/lib/jenkins/.ssh/cloudera_edge.pem` (already in place).

#### 2. Python dependencies

The pipeline reuses the venv at `/var/lib/jenkins/venvs/unit_testing_bd`.
Stage 1 installs all dependencies from `requirements.txt` automatically on first run.

### Pipeline Stages

```
Build Now (select LOAD_MODE)
        |
        v
[Stage 1] Install Dependencies
          Create/reuse venv at /var/lib/jenkins/venvs/unit_testing_bd
          pip install requirements.txt (pyspark, pytest, great-expectations)
        |
        v
[Stage 2] Unit Tests
          pytest data_ingestion/src/curated_layer/tests/ -v
          Runs 11 test classes against mock data (no cluster needed)
          Publishes JUnit XML results to Jenkins
        |
        v
[Stage 3] Curated Load: Hive Raw → Hive Curated
          Copies spark files + selected script to edge node via SCP
          SSHes to 13.41.167.97 and runs:
            - curated_full_load.sh      (LOAD_MODE=full)
            - curated_incremental_load.sh (LOAD_MODE=incremental)
        |
        v
[Stage 4] Data Quality Check
          Copies great_expectations_suite.py to edge node
          spark-submit --master yarn great_expectations_suite.py
          Fails the build if any expectation fails
        |
        v
[Post]  deleteDir() — workspace cleanup
```

### How to Run

#### Incremental load (default — daily use)

1. Go to `http://13.42.152.118:8080/job/curated-layer-ingestion/`
2. Click **Build with Parameters**
3. Leave `LOAD_MODE` as `incremental`
4. Click **Build**

#### Full load (first run or full refresh)

1. Go to `http://13.42.152.118:8080/job/curated-layer-ingestion/`
2. Click **Build with Parameters**
3. Set `LOAD_MODE` to `full`
4. Click **Build**

### Monitoring

View live console output:

```
http://13.42.152.118:8080/job/curated-layer-ingestion/lastBuild/console
```

View unit test results (JUnit report):

```
http://13.42.152.118:8080/job/curated-layer-ingestion/lastBuild/testReport/
```

### Re-enable Daily Cron (optional)

To schedule the incremental load daily at 10:00 AM, add to the Jenkinsfile:

```groovy
triggers {
    cron('0 10 * * *')
}
```

---

## Directory Structure

```
curated_layer/
├── spark/
│   ├── transformations.py        # 10 transformation functions + apply_all()
│   ├── full_load.py              # Spark job: full replace of curated table
│   └── incremental_load.py      # Spark job: append new records only
├── tests/
│   ├── conftest.py               # SparkSession fixture + mock DataFrame (8 rows)
│   └── test_transformations.py  # 11 pytest classes (one per transformation)
├── data_quality/
│   └── great_expectations_suite.py  # 14 data quality expectations
├── oozie/
│   ├── full_load/
│   │   ├── workflow.xml
│   │   └── job.properties
│   └── incremental_load/
│       ├── workflow.xml
│       └── job.properties
├── scripts/
│   ├── curated_full_load.sh
│   └── curated_incremental_load.sh
├── Jenkinsfile
├── requirements.txt
└── README.md
```

## Cluster Details

| Component | Value |
|-----------|-------|
| Edge node | `13.41.167.97` |
| NameNode | `hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020` |
| HiveServer2 | `ip-172-31-12-74.eu-west-2.compute.internal:10000` |
| Oozie | `http://ip-172-31-12-74.eu-west-2.compute.internal:11000/oozie` |
| YARN ResourceManager | `ip-172-31-3-85.eu-west-2.compute.internal:8032` |
| Raw Hive table | `bd_class_project.cc_fraud_trans` |
| Curated Hive table | `bd_class_project.cc_fraud_trans_curated` |
