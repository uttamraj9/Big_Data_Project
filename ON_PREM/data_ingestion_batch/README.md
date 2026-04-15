# data_ingestion_batch

Batch data pipeline for the credit card fraud detection project.
Ingests synthetic transaction data from PostgreSQL through HDFS into Hive,
then applies Spark transformations to produce an enriched curated dataset.

```
PostgreSQL (source)
      |
      |  Sqoop MapReduce
      v
HDFS  ──►  Hive Raw Layer       (bd_class_project.cc_fraud_trans)
                  |
                  |  PySpark — 10 transformations
                  v
           Hive Curated Layer   (bd_class_project.cc_fraud_trans_curated)
```

---

## Directory Structure

```
data_ingestion_batch/
├── src/
│   ├── raw_layer/                  Raw data landing zone (PostgreSQL → HDFS → Hive)
│   │   ├── ingest_to_postgres.py   Loads CSV splits into PostgreSQL (full/inc/stream)
│   │   ├── simulate_data_split.py  Splits raw CSV 60/20/20 by Timestamp
│   │   ├── create_raw_hive_table.hql  Hive DDL reference
│   │   ├── oozie/
│   │   │   ├── full_load/          Oozie workflow: Sqoop full import + Hive DDL
│   │   │   └── incremental_load/   Oozie coordinator: daily Sqoop append
│   │   └── README.md               Raw layer — detailed manual + Jenkins steps
│   │
│   └── curated_layer/              Spark transformation layer (Hive raw → Hive curated)
│       ├── spark/
│       │   ├── transformations.py  10 PySpark transformations
│       │   ├── full_load.py        Spark job — full overwrite of curated table
│       │   └── incremental_load.py Spark job — append new records only
│       ├── tests/                  pytest unit tests with mock SparkSession
│       ├── data_quality/           Great Expectations validation suite
│       ├── oozie/                  Oozie workflows for curated layer
│       ├── scripts/                spark-submit wrappers
│       └── README.md               Curated layer — detailed manual + Jenkins steps
│
├── data/
│   ├── raw/synthetic_fraud_dataset.csv   Original synthetic dataset
│   └── split/                            CSV subsets produced by simulate_data_split.py
│       ├── full_load.csv
│       ├── incremental_load.csv
│       └── kafka_streaming.csv
│
├── Jenkinsfile      Raw layer incremental ingestion pipeline
├── requirements.txt Python dependencies (sqlalchemy, pandas, psycopg2, python-dotenv)
└── README.md        ← this file
```

---

## Layers

### Raw Layer (`src/raw_layer/`)

Lands raw data in HDFS exactly as it comes from the source PostgreSQL database.
No transformations — data quality and schema are preserved as-is.

| Step | Tool | Description |
|------|------|-------------|
| Simulate source data | `ingest_to_postgres.py` | Appends daily transaction batches to PostgreSQL |
| Full load | Sqoop | Imports entire `cc_fraud_trans` table to HDFS, recreates Hive external table |
| Incremental load | Sqoop + watermark | Appends only rows newer than `MAX(Timestamp)` in Hive |
| Scheduling | Oozie coordinator | Runs daily at 10:00 AM UTC |

See `src/raw_layer/README.md` for full step-by-step instructions.

### Curated Layer (`src/curated_layer/`)

Reads the raw Hive table, applies 10 PySpark transformations, and writes
an enriched curated table ready for ML and analytics.

| Transformation | New Column | Description |
|----------------|-----------|-------------|
| Remove duplicates | — | Drop rows with duplicate `trans_num` |
| Handle nulls | — | Fill defaults, drop rows missing key fields |
| Time features | `txn_hour`, `txn_day_of_week`, `txn_month`, `is_weekend` | Calendar features from `Timestamp` |
| Normalize amount | `amt_log` | `log1p(amt)` to reduce skew |
| Bucket amount | `amt_bucket` | `low / medium / high / very_high` |
| Calculate age | `cardholder_age` | Age in years from `dob` |
| Calculate distance | `distance_km` | Haversine distance cardholder ↔ merchant |
| Encode gender | `gender_encoded` | `F→0`, `M→1`, other→`-1` |
| Transaction velocity | `txn_velocity_day` | Transactions per card per day (window) |
| Flag high risk | `high_risk` | `amt>500` AND `hour 0–5` AND `velocity>3` |

See `src/curated_layer/README.md` for full step-by-step instructions.

---

## Jenkins Pipelines

| Pipeline | Jenkinsfile | Description |
|----------|-------------|-------------|
| `raw-layer-ingestion` | `Jenkinsfile` | Daily incremental: PostgreSQL → HDFS → Hive |
| `curated-layer-ingestion` | `src/curated_layer/Jenkinsfile` | Unit tests → Spark load → Data quality check |

Jenkins URL: `http://13.42.152.118:8080`

---

## Quick Start

### 1. Set up PostgreSQL (Docker)

```bash
docker run -d --name my-postgres \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin123 \
  -e POSTGRES_DB=testdb \
  -p 5432:5432 postgres:15
```

### 2. Split dataset and load into PostgreSQL

```bash
cd data_ingestion_batch
pip install -r requirements.txt

# Split raw CSV → full_load / incremental / streaming subsets
python3 src/raw_layer/simulate_data_split.py

# Full load into PostgreSQL
DB_HOST=localhost DB_PORT=5432 DB_NAME=testdb \
DB_USERNAME=admin DB_PASSWORD=admin123 \
python3 src/raw_layer/ingest_to_postgres.py full
```

### 3. Run raw layer (Sqoop → HDFS → Hive)

See `src/raw_layer/README.md` — Part 1 (Manual) or Part 2 (Jenkins).

### 4. Run curated layer (Spark transformations)

See `src/curated_layer/README.md` — Part 1 (Manual) or Part 2 (Jenkins).

---

## Cluster Details

| Component | Value |
|-----------|-------|
| Edge node | `13.41.167.97` |
| NameNode | `hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020` |
| HiveServer2 | `ip-172-31-12-74.eu-west-2.compute.internal:10000` |
| Oozie | `http://ip-172-31-12-74.eu-west-2.compute.internal:11000/oozie` |
| PostgreSQL (source) | `13.42.152.118:5432` — `testdb` — `admin/admin123` |
| Raw Hive table | `bd_class_project.cc_fraud_trans` |
| Curated Hive table | `bd_class_project.cc_fraud_trans_curated` |
