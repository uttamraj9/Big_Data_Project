terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
    }
    local = {
      source = "hashicorp/local"
    }
  }
}

# ─── Use existing Synapse workspace ──────────────────────────
data "azurerm_synapse_workspace" "existing" {
  name                = var.existing_workspace_name
  resource_group_name = var.existing_workspace_rg
}

# ─── Grant Synapse Managed Identity access to ADLS ───────────
resource "azurerm_role_assignment" "synapse_adls_access" {
  scope                = var.adls_account_id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = data.azurerm_synapse_workspace.existing.identity[0].principal_id
}

# ─── Write gold-layer SQL scripts to disk ────────────────────
# The DATA_PIPELINE Jenkinsfile uploads these to Synapse Studio
# via: az synapse sql script create --workspace-name ... --file ...

resource "local_file" "gold_db_sql" {
  filename = "${path.module}/../../../../DATA_PIPELINE/cc_fraud_pipeline/synapse/01_create_gold_db.sql"
  content  = <<-SQL
    -- Run against: Built-in (Serverless SQL pool)
    IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'GoldDB')
    BEGIN
        CREATE DATABASE GoldDB;
    END
  SQL
}

resource "local_file" "gold_views_sql" {
  filename = "${path.module}/../../../../DATA_PIPELINE/cc_fraud_pipeline/synapse/02_create_gold_views.sql"
  content  = <<-SQL
    -- Run against: GoldDB (Serverless SQL pool)
    USE GoldDB;
    GO

    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
        EXEC('CREATE SCHEMA gold');
    GO

    IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'gold_adls')
    CREATE EXTERNAL DATA SOURCE gold_adls
    WITH (
        LOCATION = 'abfss://gold@${var.adls_account_name}.dfs.core.windows.net'
    );
    GO

    IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'parquet_format')
    CREATE EXTERNAL FILE FORMAT parquet_format
    WITH (FORMAT_TYPE = PARQUET);
    GO

    -- Fraud rate by merchant category
    CREATE OR ALTER VIEW gold.fraud_by_merchant AS
    SELECT
        merchant_category,
        COUNT(*)                                          AS total_txns,
        SUM(CAST(fraud_label AS INT))                     AS fraud_count,
        ROUND(AVG(CAST(fraud_label AS FLOAT)) * 100, 2)  AS fraud_rate_pct,
        ROUND(AVG(transaction_amount), 2)                 AS avg_txn_amount
    FROM OPENROWSET(
        BULK 'cc_fraud_trans/**',
        DATA_SOURCE = 'gold_adls',
        FORMAT = 'PARQUET'
    ) WITH (
        merchant_category   VARCHAR(100),
        transaction_amount  FLOAT,
        fraud_label         INT
    ) AS t
    GROUP BY merchant_category;
    GO

    -- Fraud rate by device type
    CREATE OR ALTER VIEW gold.fraud_by_device AS
    SELECT
        device_type,
        COUNT(*)                                          AS total_txns,
        SUM(CAST(fraud_label AS INT))                     AS fraud_count,
        ROUND(AVG(CAST(fraud_label AS FLOAT)) * 100, 2)  AS fraud_rate_pct
    FROM OPENROWSET(
        BULK 'cc_fraud_trans/**',
        DATA_SOURCE = 'gold_adls',
        FORMAT = 'PARQUET'
    ) WITH (
        device_type  VARCHAR(50),
        fraud_label  INT
    ) AS t
    GROUP BY device_type;
    GO

    -- Daily fraud trend
    CREATE OR ALTER VIEW gold.daily_fraud_trend AS
    SELECT
        CAST(txn_date AS DATE)                            AS txn_date,
        COUNT(*)                                          AS total_txns,
        SUM(CAST(fraud_label AS INT))                     AS fraud_count,
        ROUND(AVG(CAST(fraud_label AS FLOAT)) * 100, 2)  AS fraud_rate_pct,
        ROUND(SUM(transaction_amount), 2)                 AS total_amount
    FROM OPENROWSET(
        BULK 'cc_fraud_trans/**',
        DATA_SOURCE = 'gold_adls',
        FORMAT = 'PARQUET'
    ) WITH (
        txn_date           VARCHAR(20),
        transaction_amount FLOAT,
        fraud_label        INT
    ) AS t
    GROUP BY CAST(txn_date AS DATE);
    GO

    -- High-risk summary by location
    CREATE OR ALTER VIEW gold.high_risk_summary AS
    SELECT
        location,
        COUNT(*)                          AS high_risk_count,
        ROUND(SUM(transaction_amount), 2) AS total_amount,
        ROUND(AVG(risk_score), 4)         AS avg_risk_score
    FROM OPENROWSET(
        BULK 'cc_fraud_trans/**',
        DATA_SOURCE = 'gold_adls',
        FORMAT = 'PARQUET'
    ) WITH (
        location           VARCHAR(100),
        transaction_amount FLOAT,
        risk_score         FLOAT,
        high_risk          INT
    ) AS t
    WHERE high_risk = 1
    GROUP BY location;
    GO
  SQL
}
