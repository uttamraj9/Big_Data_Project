# Big Data Project — Credit Card Fraud Detection

End-to-end big data platform for credit card fraud detection, implemented across on-premises (Hadoop/Spark) and cloud (Azure) environments. The project ingests transaction data, applies ML-grade feature engineering, detects fraud patterns, and exposes results via Synapse Analytics.

---

## Repository Structure

```
Big_Data_Project/
├── ON_PREM/                          # On-premises Hadoop/Spark pipeline
│   ├── data_ingestion_batch/         # Batch ingestion from PostgreSQL → Hive
│   ├── data_ingestion_realtime/      # Kafka → Spark Streaming → Hive
│   └── ml/                           # Fraud detection ML models
│
└── CLOUD/
    └── AZURE/
        ├── IAC/                      # Terraform infrastructure (infra only)
        │   ├── main.tf
        │   ├── variables.tf
        │   ├── outputs.tf
        │   ├── terraform.tfvars
        │   └── modules/
        │       ├── adls/             # Azure Data Lake Storage Gen2
        │       ├── adf/              # Azure Data Factory (factory + linked services)
        │       ├── databricks/       # Azure Databricks workspace
        │       ├── keyvault/         # Azure Key Vault
        │       └── synapse/          # Azure Synapse Analytics
        │
        └── DATA_PIPELINE/
            └── cc_fraud_pipeline/    # Pipeline artifacts (deploy separately from IAC)
                ├── adf/
                │   ├── datasets/     # ADF dataset JSON definitions
                │   ├── pipelines/    # ADF pipeline JSON definitions
                │   └── triggers/     # ADF trigger JSON definitions
                ├── databricks/
                │   ├── init_watermark.py      # One-time watermark initialisation
                │   ├── raw_to_curated.py      # Incremental transform + Delta MERGE
                │   ├── curated_to_gold.py     # Gold aggregations
                │   └── jobs/                  # Databricks job definitions
                └── deploy.sh         # Deployment script
```

---

## Architecture

### Cloud Architecture (Azure)

```
PostgreSQL (EC2, eu-west-2)
        │
        │  ADF ScheduleTrigger (01:00 UTC daily)
        │  WHERE timestamp > last_watermark          ← incremental filter
        ▼
ADLS Gen2 — raw/
    cc_fraud_trans/
        ingestion_date=YYYY-MM-DD/
            cc_fraud_trans.csv
    watermark/
        cc_fraud_trans.json                          ← {"last_watermark": "..."}
        │
        │  Databricks Job (02:30 UTC)
        │  Delta MERGE on transaction_id             ← upsert (not overwrite)
        ▼
ADLS Gen2 — curated/
    cc_fraud_trans/                                  ← Delta table
        fraud_label=0/  fraud_label=1/
        _delta_log/
        │
        │  Databricks Job (03:30 UTC)
        │  Full recompute over Delta table
        ▼
ADLS Gen2 — gold/
    cc_fraud_trans/
        fraud_by_merchant/
        fraud_by_device/
        fraud_by_location/
        hourly_fraud_pattern/
        high_risk_summary/
        │
        │  Synapse Serverless SQL (OPENROWSET)
        ▼
Power BI / Reporting
```

### On-Premises Architecture

```
PostgreSQL
    │
    ├── Batch: Sqoop → HDFS raw → Spark transformations → Hive curated → Hive gold
    └── Streaming: Kafka → Spark Streaming → Hive real-time tables
                                                │
                                            ML Models (PySpark MLlib)
                                            fraud_detection_model/
```

---

## Incremental Load Design

### ADF Layer — Watermark-Based Change Tracking

| Component | Description |
|-----------|-------------|
| `DS_CC_Fraud_Watermark` | JSON dataset reading `raw/watermark/cc_fraud_trans.json` |
| `GetWatermark` (Lookup) | Reads `{"last_watermark": "YYYY-MM-DD HH:MM:SS"}` before copy |
| `CopyIncrementalData` | `WHERE timestamp > last_watermark` — copies only new rows |
| Sink partition | `raw/cc_fraud_trans/ingestion_date=YYYY-MM-DD/` |

### Databricks Layer — Delta Lake MERGE

| Step | Description |
|------|-------------|
| Read | Only today's `ingestion_date=YYYY-MM-DD` partition |
| Transform | 10 identical business rules as ON_PREM |
| Write | `DeltaTable.merge()` on `transaction_id` — update existing, insert new |
| Watermark update | `dbutils.fs.put()` writes new `max(timestamp)` back to ADLS |

### First-Run Initialisation

Before the first pipeline execution, run `init_watermark.py` once. It sets the watermark to `2000-01-01 00:00:00` so ADF copies all existing PostgreSQL data on the first run. `deploy.sh` runs this automatically.

---

## Azure Infrastructure

Managed by Terraform in `CLOUD/AZURE/IAC/`.

| Resource | Name | Details |
|----------|------|---------|
| Resource Group | `Itc_Bigdata` | North Europe |
| ADLS Gen2 | `itcbdneadls` | Containers: raw, curated, gold |
| Data Factory | `itc-bd-ne-adf` | System-assigned identity |
| Databricks | `itc-bd-ne-adb` | Premium SKU |
| Key Vault | `itc-bd-ne-kv` | RBAC mode |
| Synapse | `itc-bd-ne-syn` | Serverless SQL pool |
| SQL Server | `itc-bd-ne-sqlserver` | North Europe |
| SQL Database | `itc-bd-sqldb` | Basic tier |

### Databricks Resources

| Resource | Value |
|----------|-------|
| Workspace URL | `https://adb-7405609294150794.14.azuredatabricks.net` |
| Secret scope | `adls-scope` |
| Secret key | `adls-account-key` |
| Dev cluster | `cc-fraud-dev` — `Standard_D2ads_v6`, single-node |
| Job: raw→curated | ID `899496009869522`, runs 02:30 UTC |
| Job: curated→gold | ID `671241738264117`, runs 03:30 UTC |

---

## Deployment

### 1. Infrastructure (Terraform)

```bash
cd CLOUD/AZURE/IAC
terraform init
terraform plan -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
```

### 2. Data Pipeline Artifacts

```bash
cd CLOUD/AZURE/DATA_PIPELINE/cc_fraud_pipeline
az login
./deploy.sh
```

`deploy.sh` performs:
1. Creates/updates 3 ADF datasets (PG source, ADLS raw sink, watermark)
2. Creates/updates ADF incremental pipeline with Lookup + filtered Copy
3. Creates/updates ADF daily schedule trigger (01:00 UTC)
4. Uploads Databricks notebooks (`init_watermark`, `raw_to_curated`, `curated_to_gold`)
5. Runs `init_watermark` once to create watermark file in ADLS
6. Updates Databricks job definitions

### Prerequisites

- Azure CLI logged in: `az login`
- Databricks secret scope `adls-scope` with key `adls-account-key` (ADLS account key)
- Terraform state: `CLOUD/AZURE/IAC/terraform.tfstate`

---

## Databricks Notebooks

### `init_watermark.py`
One-time run. Creates `raw/watermark/cc_fraud_trans.json` with value `2000-01-01 00:00:00`. Automatically called by `deploy.sh`.

### `raw_to_curated.py`
Incremental batch notebook:
- Reads `raw/cc_fraud_trans/ingestion_date=YYYY-MM-DD/`
- Applies 10 feature engineering transforms
- Delta MERGE into `curated/cc_fraud_trans/` on `transaction_id`
- Updates watermark to `max(timestamp)` of processed batch
- Exits early with `NO_DATA` if partition is empty

### `curated_to_gold.py`
Gold aggregation notebook:
- Reads full `curated/cc_fraud_trans/` Delta table
- Produces 5 gold tables (fraud by merchant, device, location, hour, high-risk)

---

## CI/CD

Jenkins server: `http://13.42.152.118` (port 80 via Apache reverse proxy → 8080)

Access: Apache httpd (`httpd.service`) enabled, proxies `*:80 → localhost:8080` permanently.

---

## Access

| Group | Resource | Role |
|-------|----------|------|
| `ITC_BD_Group_FE` | `Itc_Bigdata` resource group | Contributor |
| `bdazuretraining` (removed) | — | — |

Synapse access: `uttam.kumar@informationtechconsultants.com` — Synapse Administrator + Storage Blob Data Contributor on ADLS.

---

## On-Premises Pipeline

See `ON_PREM/data_ingestion_batch/README.md` for the Hadoop/Sqoop/Hive batch pipeline and `ON_PREM/data_ingestion_realtime/README.md` for the Kafka streaming pipeline.
