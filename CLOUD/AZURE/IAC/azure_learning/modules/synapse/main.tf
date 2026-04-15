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

# ─── Synapse SQL Script: Create Gold Layer Database ───────────
# Run these manually in Synapse Studio Serverless SQL pool
# or via az synapse sql script after role assignment above

# Gold DB creation (run once via sqlcmd or Synapse Studio):
# CREATE DATABASE GoldDB;
#
# External Data Source pointing to ADLS gold container:
# CREATE EXTERNAL DATA SOURCE gold_adls
# WITH (
#   LOCATION = 'abfss://gold@<storage_account>.dfs.core.windows.net',
#   TYPE = HADOOP
# );
#
# External File Format:
# CREATE EXTERNAL FILE FORMAT parquet_format
# WITH (FORMAT_TYPE = PARQUET);
#
# Gold External Table (employees summary):
# CREATE EXTERNAL TABLE gold.employees_summary
# WITH (
#   LOCATION = 'employees_summary/',
#   DATA_SOURCE = gold_adls,
#   FILE_FORMAT = parquet_format
# )
# AS
# SELECT
#   department,
#   COUNT(*)           AS headcount,
#   AVG(salary)        AS avg_salary,
#   MIN(salary)        AS min_salary,
#   MAX(salary)        AS max_salary,
#   MIN(hire_date)     AS earliest_hire,
#   MAX(hire_date)     AS latest_hire
# FROM curated.employees
# GROUP BY department;

# ─── Synapse SQL Script resource (for documentation) ─────────
resource "azurerm_synapse_sql_script" "create_gold_db" {
  name         = "Create_GoldDB"
  synapse_workspace_id = data.azurerm_synapse_workspace.existing.id

  type = "SqlQuery"
  content = <<-SQL
    -- Run against: Built-in (Serverless SQL pool)
    IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'GoldDB')
    BEGIN
        CREATE DATABASE GoldDB;
    END
  SQL

  sql_pool_id = "Built-in"
  folder      = "Gold Layer"

  description = "Creates the GoldDB database in the Serverless SQL pool"
}

resource "azurerm_synapse_sql_script" "create_gold_objects" {
  name         = "Create_Gold_ExternalTable"
  synapse_workspace_id = data.azurerm_synapse_workspace.existing.id

  type = "SqlQuery"
  content = <<-SQL
    -- Run against: GoldDB (Serverless SQL pool)
    USE GoldDB;
    GO

    -- Create schema
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
        EXEC('CREATE SCHEMA gold');
    GO

    -- External data source pointing to ADLS gold container
    IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'gold_adls')
    CREATE EXTERNAL DATA SOURCE gold_adls
    WITH (
        LOCATION = 'abfss://gold@${var.adls_account_name}.dfs.core.windows.net'
    );
    GO

    -- Parquet file format
    IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'parquet_format')
    CREATE EXTERNAL FILE FORMAT parquet_format
    WITH (FORMAT_TYPE = PARQUET);
    GO

    -- Gold layer view: Employees department summary
    CREATE OR ALTER VIEW gold.employees_dept_summary AS
    SELECT
        department,
        COUNT(*)            AS headcount,
        ROUND(AVG(salary), 2) AS avg_salary,
        MIN(salary)         AS min_salary,
        MAX(salary)         AS max_salary,
        MIN(hire_date)      AS earliest_hire,
        MAX(hire_date)      AS latest_hire
    FROM OPENROWSET(
        BULK 'employees/**',
        DATA_SOURCE = 'gold_adls',
        FORMAT = 'PARQUET'
    )
    WITH (
        employee_id INT,
        first_name  VARCHAR(50),
        last_name   VARCHAR(50),
        department  VARCHAR(50),
        salary      DECIMAL(10,2),
        hire_date   DATE,
        is_active   BIT
    ) AS curated_data
    GROUP BY department;
    GO

    -- Gold layer view: High earners
    CREATE OR ALTER VIEW gold.high_earners AS
    SELECT *
    FROM OPENROWSET(
        BULK 'employees/**',
        DATA_SOURCE = 'gold_adls',
        FORMAT = 'PARQUET'
    )
    WITH (
        employee_id INT,
        first_name  VARCHAR(50),
        last_name   VARCHAR(50),
        department  VARCHAR(50),
        salary      DECIMAL(10,2),
        hire_date   DATE,
        is_active   BIT
    ) AS curated_data
    WHERE salary > 60000
      AND is_active = 1;
    GO
  SQL

  sql_pool_id = "Built-in"
  folder      = "Gold Layer"

  description = "Creates external data source, file format, and gold views in GoldDB"

  depends_on = [azurerm_synapse_sql_script.create_gold_db]
}
