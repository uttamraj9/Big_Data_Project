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

## Databricks cluster
- Workspace host: `adb-7405609294150794.14.azuredatabricks.net`
- Cluster: `ADBcluster`, id `0417-133347-aqen2ief`
- Spark version: 17.3.x-scala2.13 (DBR 17.3)
- Security mode: `USER_ISOLATION` (Unity Catalog Shared access)

### EventHubs Integration
**Recommended approach:** Use Event Hubs **Kafka-compatible endpoint** with Spark's built-in `kafka` source.
- No library installation required
- Works seamlessly on Shared clusters
- EventHubs-Spark connector (`azure-eventhubs-spark`) is in maintenance mode

See `DATABRICKS_EVENTHUBS_FIX.md` for:
- Complete Kafka endpoint integration code
- Alternative: Adding EventHubs Maven library to UC allowlist (requires metastore admin)
- Troubleshooting steps

Quick check cluster status:
```bash
TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv)
curl -s "https://adb-7405609294150794.14.azuredatabricks.net/api/2.0/clusters/get?cluster_id=0417-133347-aqen2ief" \
  -H "Authorization: Bearer $TOKEN" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"State: {d.get('state')} | Security: {d.get('data_security_mode')}\")"
```
