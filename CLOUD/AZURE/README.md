# Azure — Big Data Platform

Azure cloud implementation of the credit card fraud detection platform. Infrastructure is managed by Terraform (`IAC/`) and kept strictly separate from pipeline artifacts (`DATA_PIPELINE/`).

---

## Structure

```
CLOUD/AZURE/
├── IAC/                                  # Terraform — infrastructure only
│   ├── main.tf                           # Root: resource group, group access, module calls
│   ├── variables.tf                      # All input variables with descriptions
│   ├── outputs.tf                        # Key outputs (endpoints, names)
│   ├── terraform.tfvars                  # Environment values (committed, no secrets)
│   └── modules/
│       ├── adls/                         # Azure Data Lake Storage Gen2
│       ├── adf/                          # Azure Data Factory (factory + linked services)
│       ├── databricks/                   # Azure Databricks workspace (Premium)
│       ├── keyvault/                     # Azure Key Vault (RBAC mode)
│       └── synapse/                      # Azure Synapse Analytics + group access
│
└── DATA_PIPELINE/
    └── cc_fraud_pipeline/                # Pipeline artifacts — see README inside
        ├── adf/                          # ADF dataset, pipeline, trigger JSON
        ├── databricks/                   # PySpark notebooks + job definitions
        ├── synapse/                      # SQL scripts for gold views
        ├── deploy.sh                     # Deploy all pipeline artifacts
        └── README.md                     # Full pipeline documentation
```

---

## Design Principle — IAC vs DATA_PIPELINE

| IAC (Terraform) | DATA_PIPELINE (deploy.sh) |
|-----------------|--------------------------|
| Creates and manages Azure resources | Deploys logic and artifacts into those resources |
| ADF factory, linked services, Key Vault secrets | ADF datasets, pipelines, triggers |
| Databricks workspace | Databricks notebooks, job definitions |
| Synapse workspace, firewall, RBAC | Synapse SQL scripts (gold views) |
| ADLS account and containers | Data files, watermark |
| Run once, re-run on infra change | Re-run on every pipeline change |

---

## Infrastructure (IAC)

### Resources Created

| Resource | Name | Details |
|----------|------|---------|
| Resource Group | `Itc_Bigdata` | North Europe |
| ADLS Gen2 | `itcbdneadls` | Hierarchical namespace, containers: raw / curated / gold |
| Data Factory | `itc-bd-ne-adf` | System-assigned identity, Key Vault linked service, PostgreSQL + ADLS linked services |
| Databricks | `itc-bd-ne-adb` | Premium SKU (required for Unity Catalog, Delta, secret scopes) |
| Key Vault | `itc-bd-ne-kv` | RBAC mode, stores PG password + ADLS account key |
| Synapse | `itc-bd-ne-synapse` | Serverless SQL pool, managed identity granted ADLS Reader |
| SQL Server | `itc-bd-ne-sqlserver` | North Europe, admin: `sqladmin` |
| SQL Database | `itc-bd-sqldb` | Basic tier (5 DTU) |

### Terraform Modules

#### `modules/adls`
Creates the ADLS Gen2 storage account with three containers:
- `raw` — landing zone for ADF copy output
- `curated` — Delta Lake tables (processed by Databricks)
- `gold` — Parquet aggregation tables (queried by Synapse)

#### `modules/adf`
Creates the Data Factory and the following linked services:
- `LS_CC_Fraud_KeyVault` — Key Vault reference (secrets managed securely)
- `LS_CC_Fraud_PostgreSQL` — PostgreSqlV2 connection to on-prem EC2 (via null_resource + az CLI, provider only supports deprecated V1)
- `LS_CC_Fraud_ADLS` — Azure Blob Storage connection to ADLS

Pipeline artifacts (datasets, pipelines, triggers) are **not** managed by Terraform. They live in `DATA_PIPELINE/` and are deployed via `deploy.sh`.

#### `modules/databricks`
Creates the Databricks Premium workspace. Notebooks and job definitions are deployed separately via `deploy.sh`.

#### `modules/keyvault`
Creates Key Vault in RBAC mode and stores:
- `pg-password` — PostgreSQL password
- `adls-account-key` — ADLS account key (also stored in Databricks secret scope `adls-scope`)

#### `modules/synapse`
Creates the Synapse workspace and manages group access via `studio_access_groups`.

### Adding Synapse Access for a New Group

Edit [IAC/terraform.tfvars](IAC/terraform.tfvars) and add one line:

```hcl
studio_access_groups = {
  "ITC_BD_Group_FE"      = "0b22faf8-f328-4fa4-b2e6-1d0728283eee"
  "ITC_BD_Group_NewTeam" = "<object-id-here>"   # ← add this
}
```

Then run:

```bash
cd CLOUD/AZURE/IAC
terraform apply -var-file="terraform.tfvars"
```

Terraform automatically grants **Synapse Contributor** (Studio access) and **Storage Blob Data Reader** (ADLS query access) to the new group.

### Resource Group Access

`ITC_BD_Group_FE` has **Contributor** on `Itc_Bigdata` resource group — managed in `main.tf`. To change the group, update the `azuread_group` data source and `azurerm_role_assignment` in `main.tf`.

---

## Terraform Usage

### First time

```bash
cd CLOUD/AZURE/IAC
terraform init
terraform plan  -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
```

### After infra changes

```bash
terraform plan  -var-file="terraform.tfvars"   # review changes
terraform apply -var-file="terraform.tfvars"   # apply
```

### Key outputs after apply

| Output | Description |
|--------|-------------|
| `adls_storage_account_name` | ADLS account name |
| `adls_raw_container` | Raw container name |
| `adf_name` | ADF factory name |
| `databricks_workspace_url` | Databricks workspace URL |
| `key_vault_uri` | Key Vault URI |
| `synapse_serverless_endpoint` | Synapse serverless SQL endpoint |

### What Terraform does NOT manage
- ADF pipeline / dataset / trigger JSON (→ `deploy.sh`)
- Databricks notebooks and job definitions (→ `deploy.sh`)
- Databricks secret scopes and secrets (→ created manually once)
- Synapse SQL scripts / gold views (→ run manually in Synapse Studio)
- Terraform state is local (`terraform.tfstate`) — move to Azure Storage backend for team use

---

## Data Pipeline (DATA_PIPELINE)

See [DATA_PIPELINE/cc_fraud_pipeline/README.md](DATA_PIPELINE/cc_fraud_pipeline/README.md) for full pipeline documentation including:
- Full vs incremental load logic
- Watermark-based change tracking
- Delta Lake MERGE deduplication
- Databricks notebook transforms
- Synapse gold views
- Deployment instructions

### Quick deploy

```bash
cd CLOUD/AZURE/DATA_PIPELINE/cc_fraud_pipeline
az login
./deploy.sh
```

---

## Access & Security

| Principal | Resource | Role |
|-----------|----------|------|
| `ITC_BD_Group_FE` | `Itc_Bigdata` resource group | Contributor |
| `ITC_BD_Group_FE` | `itc-bd-ne-synapse` workspace | Synapse Contributor |
| `ITC_BD_Group_FE` | `itcbdneadls` storage account | Storage Blob Data Reader |
| ADF Managed Identity | `itc-bd-ne-kv` | Key Vault Secrets User |
| Synapse Managed Identity | `itcbdneadls` | Storage Blob Data Reader |

Secrets are stored in Key Vault — never in code or `terraform.tfvars`.

---

## CI/CD

Jenkins server: `http://13.42.152.118` (Apache reverse proxy → port 8080, permanent via `httpd.service`)

`Jenkinsfile` is in `DATA_PIPELINE/cc_fraud_pipeline/Jenkinsfile`.
