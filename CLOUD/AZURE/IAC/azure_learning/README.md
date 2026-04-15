# Azure Learning Project

## Overview
This project documents hands-on learning with Azure Synapse Analytics using the Azure CLI and Synapse Serverless SQL pool.

---

## Environment

| Detail | Value |
|---|---|
| **Azure Account** | uttam.kumar@InformationTechConsultants.com |
| **Subscription** | Azure subscription 1 |
| **Subscription ID** | 3a72be92-287b-4f1e-840a-5e3e71100139 |
| **Tenant** | Information Tech Consultants |
| **Tenant ID** | 2b32b1fa-7899-482e-a6de-be99c0ff5516 |

---

## Synapse Workspaces

| Workspace | Location | Resource Group | Status |
|---|---|---|---|
| `itc-bd-ne-synapse` | North Europe | Itc_Bigdata | Succeeded |
| `syn-bikestores-dev` | UK South | rg_rupali | Succeeded |
| `fetraining` | UK South | Training_AC | Succeeded |

---

## Session 1 — Synapse Access Setup

### Steps Completed

1. **Installed Azure CLI** (v2.85.0) via Homebrew
2. **Logged in** via `az login` (interactive browser login)
3. **Discovered 3 Synapse workspaces** using `az synapse workspace list`
4. **Identified permission gap** — had Azure Owner (ARM level) but no Synapse RBAC role
5. **Assigned Synapse Administrator role** via Synapse Studio → Manage → Access Control
6. **Verified access** via CLI — role assignment confirmed

### Key Learning: Two Separate Permission Systems

| Permission Type | Where Managed | What It Controls |
|---|---|---|
| Azure RBAC (IAM) | Azure Portal → Resource Group → IAM | ARM resources, billing, deployment |
| Synapse RBAC | Synapse Studio → Manage → Access Control | Pipelines, data, linked services, SQL pools |

> You can be Azure Owner and still have no access inside Synapse Studio. Both need to be configured separately.

---

## Session 1 — First Query on Serverless SQL Pool

### Workspace Details

| Detail | Value |
|---|---|
| **Workspace** | `itc-bd-ne-synapse` |
| **Serverless SQL Endpoint** | `itc-bd-ne-synapse-ondemand.sql.azuresynapse.net` |
| **Database Created** | `TestDB` |

### SQL Object Created

```sql
-- Created in: TestDB database, Serverless SQL pool
CREATE VIEW dbo.Employees AS
SELECT 1 AS EmployeeID, 'Uttam'  AS FirstName, 'Kumar'  AS LastName, 'IT'      AS Department, CAST(75000.00 AS DECIMAL(10,2)) AS Salary, CAST('2024-01-15' AS DATE) AS HireDate
UNION ALL
SELECT 2, 'Rahul',  'Sharma',  'Finance', 65000.00, CAST('2023-06-01' AS DATE)
UNION ALL
SELECT 3, 'Priya',  'Singh',   'HR',      60000.00, CAST('2022-11-20' AS DATE);
```

### Query Result

```
EmployeeID  FirstName  LastName  Department  Salary     HireDate
----------  ---------  --------  ----------  ---------  ----------
1           Uttam      Kumar     IT          75000.00   2024-01-15
2           Rahul      Sharma    Finance     65000.00   2023-06-01
3           Priya      Singh     HR          60000.00   2022-11-20
```

### Why a VIEW and not a TABLE?

Synapse **Serverless SQL pool** is a **query engine, not a storage engine**.

- It has **no disk storage** — it cannot persist physical tables
- It is designed to **query external data** stored in Azure Data Lake Storage (ADLS Gen2) in formats like Parquet, CSV, JSON
- `CREATE TABLE` is **not supported** in Serverless SQL pool
- To use real persistent tables, you need a **Dedicated SQL Pool** (billed per DWU hour)

| Pool Type | Supports CREATE TABLE | Storage | Cost |
|---|---|---|---|
| Serverless SQL Pool | No | No local storage (queries ADLS) | Pay per TB scanned |
| Dedicated SQL Pool | Yes | Managed storage | Pay per DWU/hour |

---

## Useful CLI Commands

```bash
# Login
az login

# List Synapse workspaces
az synapse workspace list --output table

# List SQL pools
az synapse sql pool list --workspace-name <workspace> --resource-group <rg> --output table

# List Spark pools
az synapse spark pool list --workspace-name <workspace> --resource-group <rg> --output table

# Check Synapse role assignments
az synapse role assignment list --workspace-name <workspace>

# Assign Synapse Administrator role
az synapse role assignment create \
  --workspace-name <workspace> \
  --role "Synapse Administrator" \
  --assignee <email>

# Connect to Serverless SQL pool
sqlcmd -S "<workspace>-ondemand.sql.azuresynapse.net,1433" -G -P "$(az account get-access-token --resource https://database.windows.net/ --query accessToken -o tsv)"
```

---

## Next Steps

- [ ] Create an ADLS Gen2 storage account and container
- [ ] Upload sample CSV/Parquet files to ADLS
- [ ] Create External Tables in Serverless SQL pool pointing to ADLS
- [ ] Create a Dedicated SQL Pool and use real `CREATE TABLE`
- [ ] Build a Synapse Pipeline for data ingestion
- [ ] Explore Spark pools for big data processing
