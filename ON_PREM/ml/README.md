# ML — Fraud Detection Pipeline

This directory contains the full machine learning pipeline for real-time credit-card fraud detection. It follows a **Lambda Architecture**: historical batch data is used to train a model, and live streaming data from HBase drives predictions every minute.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         BATCH PATH  (Training)                          │
│                                                                         │
│  Raw CSV  ──►  Hive: cc_fraud_trans                                     │
│                       │                                                 │
│               ml_transforms/                                            │
│           (StringIndexer + OHE + timestamp features)                    │
│                       │                                                 │
│               Hive: bd_class_project.ml_from_csv                        │
│                       │                                                 │
│            create_model/train_rf_from_hive.py                           │
│        (RandomForestClassifier, 100 trees, maxDepth=5)                  │
│                       │                                                 │
│            /app/model  (PySpark PipelineModel on disk)                  │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                       REAL-TIME PATH  (Prediction)                      │
│                                                                         │
│  FastAPI  ──►  Kafka: cc_fraud_stream  ──►  HBase: cc_fraud_realtime    │
│                                                       │                 │
│                                    predict/predict_from_hbase.py        │
│                                    (k8s CronJob, every 1 min)           │
│                                           │                             │
│                                     HBase scan (SingleColumnValueFilter)│
│                                           │                             │
│                                     Feature engineering                 │
│                                     (OHE, timestamp, cast)              │
│                                           │                             │
│                                     PipelineModel.transform()           │
│                                           │                             │
│                                     Hive: bd_class_project              │
│                                           .predictions_realtime         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Directory Structure

```
ml/
├── create_model/               # Step 2 — Train the RandomForest model
│   ├── train_rf_from_hive.py   # Reads ml_from_csv from Hive, trains RF, saves PipelineModel
│   ├── Dockerfile              # python:3.9-slim + PySpark 3.5.3 + Java 21
│   ├── deployment.yaml         # Kubernetes Job (rf-train) — runs once to produce model
│   └── Jenkinsfile             # CI: build rdv100/train_rf:1.5, push, kubectl apply
│
├── ml_transforms/              # Step 1 — Feature engineering from Hive to Hive
│   ├── full_ml_trans.py        # Full load: cc_fraud_trans → ml_from_csv (StringIndexer + OHE)
│   ├── incr_ml_trans.py        # Incremental load: process only new rows since last run
│   ├── Jenkinsfile             # Scheduled every 12h; runs full on first run, incr thereafter
│   └── other_ways/             # Alternative implementations (kept for reference)
│       ├── Transformations.py
│       ├── ml_transform.py
│       └── ml_transform_improve.py
│
└── predict/                    # Step 3 — Real-time inference (production)
    ├── predict_from_hbase.py   # MAIN: HBase scan → feature eng → RF model → Hive
    ├── predict_from_hive.py    # Legacy: Hive-based prediction (kept for reference)
    ├── test_predict_pipeline.py # 6-test integration test suite
    ├── Dockerfile              # python:3.9-slim + PySpark 3.5.3 + happybase + Java 21
    ├── predict-cron.yaml       # Kubernetes CronJob (*/1 * * * *)
    ├── deployment.yaml         # Kubernetes one-shot Job for manual runs
    └── Jenkinsfile             # CI: build rdv100/predict_fraud:2.8, push, kubectl apply
```

---

## Infrastructure

| Component | Address | Notes |
|-----------|---------|-------|
| Hive Metastore | `thrift://172.31.6.42:9083` | Cloudera CDH 7.1.7 |
| HBase Thrift Server | `172.31.6.42:9090` | Started as systemd service `hbase-thrift` |
| HDFS NameNode | `172.31.3.251:9000` | Warehouse at `/warehouse/tablespace/external/hive/` |
| Kafka Brokers | `172.31.3.85:9092`, `172.31.6.42:9092` | Topic: `cc_fraud_stream` |
| Kubernetes Node | `172.31.8.235` | All ML pods pinned via `nodeSelector` |
| Model on disk | `/home/ec2-user/BigData_May/bd_class_project/ml/all_ml/model` | Mounted as `hostPath` into pods |
| Docker Hub | `rdv100/predict_fraud:2.8` | Prediction image |
| Docker Hub | `rdv100/train_rf:1.5` | Training image |

---

## Step 1 — ML Transforms (`ml_transforms/`)

Reads raw transactions from Hive (`bd_class_project.cc_fraud_trans`), applies feature engineering, and writes the ML-ready table (`bd_class_project.ml_from_csv`).

### What it does

| Transform | Detail |
|-----------|--------|
| Dedup + null drop | `dropDuplicates()`, `na.drop()` |
| `user_id` | Strip `USER_` prefix, cast to `int` |
| Categorical OHE | `StringIndexer` → `OneHotEncoder` → flatten to named binary columns |
| Timestamp features | Parse `Timestamp`, extract `Hour`, `DayOfWeek`, `DayOfMonth` |
| Drop `transaction_id` | Not used as a model feature |

### Two modes

- **`full_ml_trans.py`** — processes the entire `cc_fraud_trans` table and overwrites `ml_from_csv`.
- **`incr_ml_trans.py`** — reads current count of `ml_from_csv`, then processes only rows in `cc_fraud_trans` beyond that index (append mode).

### Jenkins pipeline

The `Jenkinsfile` runs on a 12-hour cron schedule. On first execution it runs the full load and creates a `full_load_done.txt` marker file. Subsequent runs use the incremental script.

---

## Step 2 — Model Training (`create_model/`)

### Script: `train_rf_from_hive.py`

Reads `bd_class_project.ml_from_csv` from Hive and trains a `RandomForestClassifier`.

```
Hive: bd_class_project.ml_from_csv
         │
         ▼
  Drop raw categorical columns (transaction_type, device_type,
  location, merchant_category, card_type, authentication_method, Timestamp)
         │
         ▼
  VectorAssembler  →  features vector  (handleInvalid="keep")
         │
         ▼
  RandomForestClassifier
    numTrees = 100
    maxDepth = 5
    labelCol = fraud_label
    seed     = 42
         │
         ▼
  70/30 train-test split  →  evaluate AUC
         │
         ▼
  PipelineModel.save("/app/output")
  (mounted as hostPath → /home/ec2-user/.../model)
```

The saved model contains two stages: `VectorAssembler` + `RandomForestClassificationModel`. The model is persisted to the EC2 host path and reused by the prediction service without retraining.

### Kubernetes Job

`deployment.yaml` defines a one-shot `Job` (`rf-train`) that:
- Runs on the k8s node `ip-172-31-8-235`
- Mounts `/home/ec2-user/BigData_May/bd_class_project/ml/all_ml/model` as `/app/output`
- Sets `HIVE_METASTORE_URIS=thrift://172.31.6.42:9083`

### Jenkins pipeline

Clones `feature/ml-model` branch, builds `rdv100/train_rf:1.5`, pushes to Docker Hub, runs `kubectl apply`.

---

## Step 3 — Real-Time Prediction (`predict/`)

This is the production component. It runs as a **Kubernetes CronJob every minute**, scanning HBase for new transactions, running them through the trained model, and writing results to Hive.

### Script: `predict_from_hbase.py`

#### Configuration (environment variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `HBASE_HOST` | `172.31.6.42` | HBase Thrift Server host |
| `HBASE_PORT` | `9090` | HBase Thrift port |
| `HBASE_TABLE` | `cc_fraud_realtime` | Source HBase table |
| `HIVE_METASTORE_URIS` | `thrift://172.31.6.42:9083` | Hive Metastore URI |
| `MODEL_PATH` | `file:///app/model` | Path to saved PipelineModel |
| `PREDICTIONS_DB` | `bd_class_project` | Output Hive database |
| `PREDICTIONS_TABLE` | `predictions_realtime` | Output Hive table |
| `HADOOP_USER_NAME` | `hive` | HDFS user identity (avoids permission denied) |

#### Execution flow

```
1. Watermark
   └─ Query Hive predictions_realtime for MAX(timestamp)
   └─ Default: '1970-01-01 00:00:00' on first run

2. HBase Scan
   └─ happybase.Connection → cc_fraud_realtime
   └─ Server-side filter:
      SingleColumnValueFilter('cf', 'Timestamp', >, 'binary:<watermark>', true, true)
   └─ Returns only rows newer than last prediction

3. Build Spark DataFrame
   └─ RAW_SCHEMA: transaction_id + 19 string columns (HBase stores all as strings)
   └─ Filter to known schema columns only

4. Feature Engineering
   ├─ Cast numeric columns to double
   │     transaction_amount, account_balance, card_age, transaction_distance,
   │     risk_score, ip_address_flag, previous_fraudulent_activity,
   │     daily_transaction_count, avg_transaction_amount_7d,
   │     failed_transaction_count_7d, is_weekend
   ├─ user_id: strip 'USER_' prefix → cast to int
   ├─ Timestamp: parse → extract hour, dayofweek, dayofmonth
   └─ Manual OHE binary columns (must match training schema exactly):
         transaction_type  → pos, bank_transfer, online, atm_withdrawal
         device_type       → mobile, tablet, laptop
         location          → tokyo, mumbai, london, sydney, new_york
         merchant_category → restaurants, clothing, travel, groceries, electronics
         card_type         → mastercard, amex, discover, visa
         authentication_method → pin, password, biometric, otp
      Normalisation: lower() + regexp_replace("[\\s-]+", "_")
      Result: 41 feature columns total

5. Model Inference
   └─ PipelineModel.load(MODEL_PATH)
   └─ Stages: VectorAssembler → RandomForestClassificationModel
   └─ pipeline.transform(features_df)
   └─ Output columns: prediction (0.0 = legitimate, 1.0 = fraud), probability

6. Join + Write
   └─ Join predictions back to raw_df on transaction_id
   └─ out_df.write.mode("append").format("hive").saveAsTable(predictions_realtime)
   └─ On first run: creates table. Subsequent runs: append only new predictions.
```

#### Feature columns (41 total)

```
Numeric (11):  transaction_amount, account_balance, card_age, transaction_distance,
               risk_score, ip_address_flag, previous_fraudulent_activity,
               daily_transaction_count, avg_transaction_amount_7d,
               failed_transaction_count_7d, is_weekend

Timestamp (3): hour, dayofweek, dayofmonth

Identity (1):  user_id (int, USER_ prefix stripped)

OHE (24):      transaction_type_pos, transaction_type_bank_transfer,
               transaction_type_online, transaction_type_atm_withdrawal,
               device_type_mobile, device_type_tablet, device_type_laptop,
               location_tokyo, location_mumbai, location_london,
               location_sydney, location_new_york,
               merchant_category_restaurants, merchant_category_clothing,
               merchant_category_travel, merchant_category_groceries,
               merchant_category_electronics,
               card_type_mastercard, card_type_amex,
               card_type_discover, card_type_visa,
               authentication_method_pin, authentication_method_password,
               authentication_method_biometric, authentication_method_otp
```

### Kubernetes CronJob: `predict-cron.yaml`

```yaml
schedule: "*/1 * * * *"        # runs every minute
concurrencyPolicy: Forbid       # skip if previous run still active
image: rdv100/predict_fraud:2.8
nodeSelector: ip-172-31-8-235   # same node as model hostPath
hostAliases:                    # DNS override — pods can't resolve internal AWS hostnames
  172.31.6.42  → ip-172-31-6-42.eu-west-2.compute.internal   (HBase/Hive)
  172.31.3.251 → ip-172-31-3-251.eu-west-2.compute.internal  (HDFS NameNode)
  172.31.3.85  → ip-172-31-3-85.eu-west-2.compute.internal   (secondary node)
volumeMount: /app/model ← /home/ec2-user/BigData_May/.../model  (hostPath)
```

### Jenkins pipeline: `Jenkinsfile`

```
Clone feature/ml-predict (uttamraj9/Big_Data_Project)
  │
  ▼
docker build -t rdv100/predict_fraud:2.8 -f ml/predict/Dockerfile ml/predict
  │
  ▼
docker push rdv100/predict_fraud:2.8
  │
  ▼
kubectl delete cronjob fraud-predict-cron --ignore-not-found
kubectl apply -f ml/predict/predict-cron.yaml
```

Credentials used:
- `kubeconfig-file` — k8s cluster access
- `rdv-hub` — Docker Hub login (`rdv100`)

---

## Docker Images

Both images use `python:3.9-slim` + `default-jre-headless` (Java 21) + PySpark 3.5.3.

> **Note:** PySpark 3.2.x is incompatible with Java 17+. Java 21 (`default-jre-headless` on Debian Trixie) requires PySpark 3.5.x. Models saved by older PySpark versions load correctly in 3.5.x.

### `ml/predict/Dockerfile`

```dockerfile
FROM python:3.9-slim
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python3
RUN pip install --no-cache-dir pyspark==3.5.3 happybase==1.2.0 numpy
COPY predict_from_hbase.py .
COPY test_predict_pipeline.py .
CMD ["spark-submit", "--master", "local[*]", "predict_from_hbase.py"]
```

### `ml/create_model/Dockerfile`

```dockerfile
FROM python:3.9-slim
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python3
RUN pip install --no-cache-dir pyspark==3.5.3 numpy
COPY train_rf_from_hive.py .
CMD ["spark-submit", "--master", "local[*]", "train_rf_from_hive.py"]
```

---

## Integration Tests (`predict/test_predict_pipeline.py`)

Six tests, run inside the prediction container:

```bash
docker run --rm \
  -v /path/to/model:/app/model \
  -v /path/to/test_predict_pipeline.py:/app/test_predict_pipeline.py \
  --network host \
  rdv100/predict_fraud:2.8 \
  spark-submit --master local[*] test_predict_pipeline.py
```

| # | Test | What it checks |
|---|------|---------------|
| 1 | HBase Thrift connectivity | Connects to `172.31.6.42:9090`, verifies `cc_fraud_realtime` table exists |
| 2 | Spark initialisation | `SparkSession` starts, version reported |
| 3 | Schema + DataFrame creation | 3 mock HBase rows → 20-column DataFrame |
| 4 | Feature engineering | 41 output columns, OHE correct, `user_id` cast to int, timestamp features present |
| 5 | Model loading | `PipelineModel.load()` succeeds, 2 stages: `VectorAssembler` + `RandomForestClassificationModel` |
| 6 | End-to-end inference | `pipeline.transform()` produces `prediction` + `probability` for all 3 rows |

Expected result when all services are running: **6 PASSED / 0 FAILED**.

Mock data covers three cases:
- `TXN_TEST_001` — low-risk transaction (New York, Bank Transfer, low amount) → prediction: `0.0`
- `TXN_TEST_002` — medium-risk (Sydney, Online, high risk_score 0.85) → prediction: `1.0`
- `TXN_TEST_003` — high-risk (Tokyo, ATM, ip_flag=1, prev_fraud=1) → prediction: `1.0`

---

## HBase Thrift Server

The Thrift server is required for the prediction pipeline. It is started as a systemd service on the Cloudera node (`172.31.6.42`) and configured to auto-start on reboot.

```bash
# Service file: /etc/systemd/system/hbase-thrift.service
# Managed via:
sudo systemctl status hbase-thrift
sudo systemctl start  hbase-thrift
sudo systemctl stop   hbase-thrift
```

To start manually if the service is not available:
```bash
ssh -i test_key.pem ec2-user@172.31.6.42
sudo -u hbase /usr/bin/hbase thrift start -p 9090 -threadpool &
```

---

## Data Tables

| Table | Location | Description |
|-------|----------|-------------|
| `bd_class_project.cc_fraud_trans` | Hive | Raw CSV transactions (training source) |
| `bd_class_project.ml_from_csv` | Hive | Feature-engineered training data |
| `cc_fraud_realtime` | HBase | Live transactions from Kafka consumer |
| `bd_class_project.predictions_realtime` | Hive (HDFS Parquet) | Output: raw fields + `prediction` column |

Output schema of `predictions_realtime`:

```
transaction_id        string
user_id               string
transaction_amount    string
transaction_type      string
timestamp             string
account_balance       string
device_type           string
location              string
merchant_category     string
ip_address_flag       string
previous_fraudulent_activity  string
daily_transaction_count       string
avg_transaction_amount_7d     string
failed_transaction_count_7d   string
card_type             string
card_age              string
transaction_distance  string
authentication_method string
risk_score            string
is_weekend            string
prediction            double    ← 0.0 = legitimate, 1.0 = fraud
```

---

## Running the Pipeline End-to-End

### First-time setup

1. **Run ML transforms** (creates `ml_from_csv`):
   ```bash
   spark-submit --master local[*] ml/ml_transforms/full_ml_trans.py
   ```

2. **Train model** — trigger Jenkins job `model-train` or run manually:
   ```bash
   kubectl apply -f ml/create_model/deployment.yaml
   kubectl logs -f job/rf-train
   # Model saved to /home/ec2-user/BigData_May/bd_class_project/ml/all_ml/model
   ```

3. **Start HBase Thrift** on Cloudera node:
   ```bash
   sudo systemctl start hbase-thrift
   ```

4. **Start Kafka producer and consumer** (in `data_ingestion_realtime/`):
   ```bash
   python3 kafka_producer.py --config ../config/config.properties
   python3 kafka_consumer.py --config ../config/config.properties
   ```

5. **Deploy prediction CronJob** — trigger Jenkins job `fraud-prediction-cron-job` or:
   ```bash
   kubectl apply -f ml/predict/predict-cron.yaml
   ```

### Ongoing operation

The CronJob runs every minute automatically. Monitor with:
```bash
kubectl get jobs --sort-by=.metadata.creationTimestamp | grep fraud
kubectl logs job/<latest-fraud-predict-cron-job>
```

Check predictions in Hive:
```bash
beeline -u 'jdbc:hive2://172.31.6.42:10000/bd_class_project' \
  -e 'SELECT COUNT(*), SUM(CASE WHEN prediction=1.0 THEN 1 ELSE 0 END) as fraud
      FROM predictions_realtime;'
```

---

## Deployment Status

| Component | Image | Status |
|-----------|-------|--------|
| Training | `rdv100/train_rf:1.5` | Built and pushed |
| Prediction | `rdv100/predict_fraud:2.8` | Built and pushed (Jenkins build #23) |
| CronJob | `fraud-predict-cron` | Running every minute on k8s |
| Integration tests | — | 6/6 PASS |
| HBase Thrift | port 9090 | Running as systemd service on 172.31.6.42 |
| Predictions table | `bd_class_project.predictions_realtime` | Populated (33,000 rows, 4.5 MB Parquet) |
