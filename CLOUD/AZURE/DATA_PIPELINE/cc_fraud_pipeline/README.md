# cc_fraud_pipeline

Azure data pipeline for credit card fraud transaction ingestion, transformation, and analytics. Ingests data from PostgreSQL (on-prem EC2), processes it through three layers (raw → curated → gold), and exposes results in Synapse Analytics.

---

## Folder Structure

```
cc_fraud_pipeline/
├── adf/
│   ├── datasets/
│   │   ├── DS_CC_Fraud_Trans_PG.json          # Source: PostgreSQL cc_fraud_trans table
│   │   ├── DS_CC_Fraud_Trans_ADLS_Raw.json    # Sink: ADLS raw layer (date-partitioned)
│   │   └── DS_CC_Fraud_Watermark.json         # Watermark: raw/watermark/cc_fraud_trans.json
│   ├── pipelines/
│   │   └── PL_CC_Fraud_Trans_PG_To_Raw.json   # Main pipeline (full + incremental)
│   └── triggers/
│       ├── TR_CC_Fraud_Daily_Ingest.json       # Daily at 01:00 UTC (incremental)
│       └── TR_CC_Fraud_Full_Load.json          # Manual (full load, kept paused)
│
├── databricks/
│   ├── init_watermark.py                       # One-time: seed watermark in ADLS
│   ├── raw_to_curated.py                       # Transform + Delta MERGE (incremental & full)
│   ├── curated_to_gold.py                      # Gold aggregations from Delta table
│   └── jobs/
│       ├── cc_fraud_raw_to_curated_job.json    # Daily job at 02:30 UTC
│       ├── cc_fraud_curated_to_gold_job.json   # Daily job at 03:30 UTC
│       └── cc_fraud_full_load_job.json         # Manual full load job
│
├── synapse/
│   ├── 01_create_gold_db.sql                  # Create GoldDB (run once)
│   └── 02_create_gold_views.sql               # Create gold views over ADLS Parquet
│
├── deploy.sh                                   # Deploy all artifacts to Azure
└── README.md
```

---

## Pipeline Architecture

```
PostgreSQL — cc_fraud_trans (EC2 13.42.152.118:5432)
        │
        │  ADF: PL_CC_Fraud_Trans_PG_To_Raw
        │  ┌─ GetWatermark (Lookup)
        │  │   reads raw/watermark/cc_fraud_trans.json
        │  │   → {"last_watermark": "2026-04-15 01:00:00"}
        │  │
        │  └─ IfCondition: load_type == "full"?
        │      ├── YES → Copy_Full_Load
        │      │         SELECT * FROM cc_fraud_trans
        │      │         → raw/cc_fraud_trans/ingestion_date=full_load/
        │      │
        │      └── NO  → Copy_Incremental
        │                WHERE timestamp > last_watermark
        │                → raw/cc_fraud_trans/ingestion_date=YYYY-MM-DD/
        │
        ▼
ADLS Gen2 — raw/
    cc_fraud_trans/
        ingestion_date=2026-04-15/cc_fraud_trans.csv   ← daily incremental
        ingestion_date=2026-04-16/cc_fraud_trans.csv
        ingestion_date=full_load/cc_fraud_trans.csv    ← full load (overwritten each time)
    watermark/
        cc_fraud_trans.json  → {"last_watermark": "2026-04-16 01:00:00"}
        │
        │  Databricks: raw_to_curated (02:30 UTC)
        │  reads ingestion_date=YYYY-MM-DD/ partition
        │  applies 10 feature engineering transforms
        │  Delta MERGE on transaction_id (upsert)
        │  advances watermark to max(timestamp)
        ▼
ADLS Gen2 — curated/
    cc_fraud_trans/           ← Delta table (partitioned by fraud_label)
        fraud_label=0/
        fraud_label=1/
        _delta_log/
        │
        │  Databricks: curated_to_gold (03:30 UTC)
        │  reads full Delta table
        │  computes 5 aggregation tables
        ▼
ADLS Gen2 — gold/
    cc_fraud_trans/
        fraud_by_merchant/
        fraud_by_device/
        fraud_by_location/
        hourly_fraud_pattern/
        high_risk_summary/
        │
        │  Synapse Serverless SQL — GoldDB
        │  views via OPENROWSET over gold Parquet
        ▼
Power BI / Reporting / ML
```

---

## Incremental Load — How It Works

### What determines "new" data
The `timestamp` column in PostgreSQL records when each transaction row was inserted or updated. The watermark file stores the `max(timestamp)` from the last successful run.

```
Day 1 run:  WHERE timestamp > '2000-01-01 00:00:00'  →  copies all 35,000 rows
            watermark updated to: '2026-04-15 01:00:00'

Day 2 run:  WHERE timestamp > '2026-04-15 01:00:00'  →  copies only new rows since yesterday
            watermark updated to: '2026-04-16 01:00:00'
```

### Why there are no duplicates

| Layer | Mechanism |
|-------|-----------|
| **ADF → raw** | `WHERE timestamp > last_watermark` — PostgreSQL returns only rows added after last run |
| **raw partitions** | Each daily run writes to its own `ingestion_date=YYYY-MM-DD/` — files never overwrite each other |
| **Databricks → curated** | Delta Lake `MERGE ON transaction_id` — same ID → **UPDATE**, new ID → **INSERT**. Even if ADF re-runs, no duplicate rows |
| **Watermark** | Only advances after Databricks successfully processes the batch — if Databricks fails, next run re-processes from the same point (no data loss, no gap) |

### What happens on a full load run
- ADF writes to `ingestion_date=full_load/` — **overwrites** the same file, not a new copy
- Databricks Delta MERGE runs over all rows — existing records updated, no second copies inserted
- Watermark is **NOT updated** — the daily incremental position is preserved after a full reload

---

## ADF Pipeline — PL_CC_Fraud_Trans_PG_To_Raw

### Parameter

| Parameter | Default | Values |
|-----------|---------|--------|
| `load_type` | `incremental` | `incremental` or `full` |

### Activities

```
GetWatermark (Lookup)
    └── reads DS_CC_Fraud_Watermark → last_watermark value

FullOrIncremental (If Condition)
    ├── load_type == "full"
    │     Copy_Full_Load
    │       source query : SELECT * FROM cc_fraud_trans ORDER BY timestamp
    │       sink         : raw/cc_fraud_trans/ingestion_date=full_load/
    │
    └── load_type == "incremental"  (default)
          Copy_Incremental
            source query : WHERE timestamp > '<last_watermark>' ORDER BY timestamp
            sink         : raw/cc_fraud_trans/ingestion_date=<today>/
```

### Triggers

| Trigger | Schedule | load_type | Action needed |
|---------|----------|-----------|---------------|
| `TR_CC_Fraud_Daily_Ingest` | Daily 01:00 UTC | `incremental` | Runs automatically |
| `TR_CC_Fraud_Full_Load` | Manual (kept paused) | `full` | Start manually when needed |

### Running a manual pipeline
ADF Studio → open pipeline → **Trigger** → **Trigger now** → set `load_type` to `full` or `incremental` → OK.

---

## Databricks Notebooks

### init_watermark.py
**Run once** before the first pipeline execution. Creates `raw/watermark/cc_fraud_trans.json` with `{"last_watermark": "2000-01-01 00:00:00"}` so the first ADF run copies all existing data. Called automatically by `deploy.sh`.

### raw_to_curated.py

**Widget parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `adls_account_name` | — | ADLS storage account name |
| `raw_container` | `raw` | Raw container name |
| `curated_container` | `curated` | Curated container name |
| `table_name` | `cc_fraud_trans` | Table name |
| `ingestion_date` | today's date | Partition to read (`full_load` for full load job) |

**Transformations applied (identical to ON_PREM):**

| Step | Transform | Logic |
|------|-----------|-------|
| 1 | `remove_duplicates` | `dropDuplicates(["transaction_id"])` |
| 2 | `handle_nulls` | Drop rows missing key columns; fill defaults for amounts/categories |
| 3 | `extract_time_features` | `txn_hour`, `txn_day_of_week`, `txn_month`, `is_weekend` |
| 4 | `normalize_amount` | `amt_log = log1p(transaction_amount)` |
| 5 | `bucket_amount` | `amt_bucket`: low / medium / high / very_high |
| 6 | `calculate_age` | `cardholder_age` from `card_age` (months) |
| 7 | `calculate_distance` | `distance_km` from `transaction_distance` |
| 8 | `encode_gender` | `gender_encoded`: biometric auth → 1, others → 0 |
| 9 | `transaction_velocity` | `txn_velocity_day`: count per user per day |
| 10 | `flag_high_risk` | `high_risk=1` if amount > 500 AND hour 0–5 AND velocity ≥ 3 |

**Write:** Delta MERGE on `transaction_id` into `curated/cc_fraud_trans/`

**Watermark:** Advanced to `max(timestamp)` after each incremental run. Skipped for `ingestion_date=full_load`.

### curated_to_gold.py
Reads the full curated Delta table and writes 5 gold aggregation tables:

| Gold Table | Description |
|------------|-------------|
| `fraud_by_merchant/` | Fraud rate, count, avg/total amount by merchant category |
| `fraud_by_device/` | Fraud rate by device type |
| `fraud_by_location/` | Fraud rate, avg amount by location |
| `hourly_fraud_pattern/` | Fraud rate by hour of day (0–23) |
| `high_risk_summary/` | High-risk transaction stats by location |

Gold tables are always fully overwritten — they are aggregation snapshots, not raw records.

---

## Databricks Jobs

| Job | Schedule | ingestion_date | Purpose |
|-----|----------|----------------|---------|
| `cc-fraud-raw-to-curated` | 02:30 UTC daily | today's date | Daily incremental processing |
| `cc-fraud-curated-to-gold` | 03:30 UTC daily | — | Daily gold aggregation refresh |
| `cc-fraud-full-load` | Manual (no schedule) | `full_load` | Full reload of curated Delta table |

---

## Synapse Analytics

Run these SQL scripts once in Synapse Studio against the **Built-in (Serverless SQL)** pool:

```sql
-- Step 1: create database
01_create_gold_db.sql

-- Step 2: create schema, external data source, views
02_create_gold_views.sql
```

Views exposed in `GoldDB.gold` schema: `fraud_by_merchant`, `fraud_by_device`, `daily_fraud_trend`, `high_risk_summary`.

---

## Deployment

### Prerequisites
- `az login` with access to subscription `3a72be92-287b-4f1e-840a-5e3e71100139`
- Databricks secret scope `adls-scope` with key `adls-account-key` must exist
- Terraform infrastructure already applied (`CLOUD/AZURE/IAC/`)

### Deploy all artifacts

```bash
cd CLOUD/AZURE/DATA_PIPELINE/cc_fraud_pipeline
./deploy.sh
```

`deploy.sh` steps:
1. Creates/updates 3 ADF datasets
2. Creates/updates ADF pipeline (`PL_CC_Fraud_Trans_PG_To_Raw`)
3. Creates/updates ADF daily trigger and starts it
4. Uploads all Databricks notebooks to `/Shared/cc_fraud_pipeline/`
5. Runs `init_watermark` once to seed the watermark file
6. Updates Databricks job definitions

### Daily schedule (automatic after deploy)

```
01:00 UTC  — ADF copies new PostgreSQL rows → ADLS raw (incremental partition)
02:30 UTC  — Databricks raw_to_curated processes partition → Delta MERGE → advances watermark
03:30 UTC  — Databricks curated_to_gold recomputes 5 gold tables
```

### Running a full reload manually

1. ADF Studio → **Manage** → **Triggers** → `TR_CC_Fraud_Full_Load` → **Start**
2. Databricks → **Jobs** → `cc-fraud-full-load` → **Run now**
3. Stop the full load trigger after it fires

---

## Configuration

| Setting | Value |
|---------|-------|
| PostgreSQL host | `13.42.152.118:5432` |
| PostgreSQL database | `testdb` |
| ADLS account | `itcbdneadls` |
| ADF name | `itc-bd-ne-adf` |
| Databricks workspace | `https://adb-7405609294150794.14.azuredatabricks.net` |
| Databricks secret scope | `adls-scope` |
| Resource group | `Itc_Bigdata` |
