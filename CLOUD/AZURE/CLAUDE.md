# CLOUD/AZURE — Project Memory

Azure side of the Credit Card Fraud Detection big data platform.
Flow: PostgreSQL (ON_PREM) → ADF → ADLS raw → Databricks (raw→curated→gold Delta) → Synapse gold views.

## Environment / how it was built
- The Azure CLI is set up **on Uttam's laptop only**. Databricks, ADF and the rest of
  the pipeline were created from there. Claude's sandbox has **no `az` access** — to query
  live resources, Uttam runs the command locally and pastes output.
- Resources provisioned via Terraform in `IAC/` (root + 5 modules: adls, adf, databricks, keyvault, synapse).

## Azure resources (from IAC/terraform.tfvars)
- Subscription: `3a72be92-287b-4f1e-840a-5e3e71100139`
- Tenant: `2b32b1fa-7899-482e-a6de-be99c0ff5516`
- Region: `northeurope`
- Resource group: `Itc_Bigdata`
- ADLS Gen2 account: `itcbdneadls`
- Data Factory: `itc-bd-ne-adf`
- Key Vault: `itc-bd-ne-kv`
- Databricks workspace: `itc-bd-ne-adb`
- Synapse workspace: `itc-bd-ne-synapse` (SQL admin: `sqladmin`)

## Access groups (Entra)
- `ITC_BD_Group_FE` = `0b22faf8-f328-4fa4-b2e6-1d0728283eee`
  - Granted: Synapse Contributor + ADLS Reader, and Key Vault Secrets Officer.
  - To add a group: edit `studio_access_groups` / `kv_secrets_officer_groups` in tfvars, then `terraform apply`.

## Source PostgreSQL (ON_PREM)
- Host `13.42.152.118:5432`, db `testdb`. Source for ADF ingestion.

## Layout
- `IAC/` — Terraform (infra only). `terraform.tfvars` holds all names/secrets.
- `DATA_PIPELINE/cc_fraud_pipeline/` — deployed separately from IAC:
  - `adf/` datasets, pipeline `PL_CC_Fraud_Trans_PG_To_Raw`, triggers (daily ingest + full load)
  - `databricks/` `init_watermark.py`, `raw_to_curated.py`, `curated_to_gold.py` + job JSONs
  - `synapse/` `01_create_gold_db.sql`, `02_create_gold_views.sql`
  - `deploy.sh`, `Jenkinsfile` for CI/CD

## Useful local commands (run on Uttam's laptop)
- `az resource list -g Itc_Bigdata -o table` — list deployed resources
- `cd CLOUD/AZURE/IAC && terraform plan` — check infra drift

> ⚠️ Secrets (Synapse SQL password, PG creds) live in plaintext in `IAC/terraform.tfvars`.
> Consider moving to Key Vault / a tfvars not committed to git.

---

## Working setup note
- Uttam works from **VS Code on the laptop** using the Azure/Databricks Cloud extensions —
  that environment has full `az` + Databricks CLI auth and can do everything.
- Claude's sandbox is isolated: **no `az`, no Databricks CLI, no laptop access.** It can only
  read/write the Downloads folder. Give Uttam commands to run locally; don't try to run cloud CLI here.

## Databricks cluster (EventHubs library issue)
- Workspace host: `adb-7405609294150794.14.azuredatabricks.net`
- Cluster: `ADBcluster`, id `0417-133347-aqen2ief`

### Problem
Installing Maven lib for Event Hubs failed with two bugs:
1. Coordinate had version `undefined`: `com.microsoft.azure:azure-eventhubs-spark_2.12:undefined`.
2. `PERMISSION_DENIED ... not in the artifact allowlist` → cluster is UC **Shared** access mode,
   where Maven/JAR libs must be on the metastore allowlist.

### Fix / runbook (run in VS Code terminal on laptop)
1. Check access mode:
   `databricks clusters get 0417-133347-aqen2ief | grep -i data_security_mode`
   - `USER_ISOLATION` = Shared → needs allowlist (step 2)
   - `SINGLE_USER`/`NONE` = allowlist N/A → go to step 3
2. If Shared, as **metastore admin** run SQL:
   `ALTER METASTORE ADD ARTIFACT 'com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22' MAVEN;`
3. Install with a valid version (2.3.22 is latest; use `_2.12` for Scala 2.12 runtime, `_2.13` for 2.13):
   `databricks libraries install --cluster-id 0417-133347-aqen2ief --maven-coordinates com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`
4. Verify: `databricks libraries cluster-status --cluster-id 0417-133347-aqen2ief`

### Alternative (cleanest)
Event Hubs exposes a **Kafka-compatible endpoint** → use Spark's built-in `kafka` source.
No library install, works on Shared clusters. eventhubs-spark connector is in maintenance mode anyway.
