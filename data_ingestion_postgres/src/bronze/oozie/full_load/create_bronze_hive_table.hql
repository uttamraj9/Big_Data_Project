-- =============================================================================
-- create_bronze_hive_table.hql  (Oozie version)
-- =============================================================================
-- Called by the bronze-full-load Oozie workflow (hive2 action).
--
-- hdfs_target_dir is injected at runtime via the Oozie <param> element
-- in workflow.xml and resolved by Hive variable substitution.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS bd_class_project;

USE bd_class_project;

-- Drop first so schema is always in sync with the PostgreSQL source
DROP TABLE IF EXISTS cc_fraud_trans;

-- EXTERNAL table: dropping this table in Hive will NOT delete the HDFS files.
-- The bronze data is owned by the Sqoop ingestion job, not Hive.
CREATE EXTERNAL TABLE bd_class_project.cc_fraud_trans (
    Transaction_ID               STRING,
    User_ID                      STRING,
    Transaction_Amount           DECIMAL(10,2),
    Transaction_Type             STRING,
    `Timestamp`                  TIMESTAMP,
    Account_Balance              DECIMAL(10,2),
    Device_Type                  STRING,
    Location                     STRING,
    Merchant_Category            STRING,
    IP_Address_Flag              INT,
    Previous_Fraudulent_Activity INT,
    Daily_Transaction_Count      INT,
    Avg_Transaction_Amount_7d    DECIMAL(10,2),
    Failed_Transaction_Count_7d  INT,
    Card_Type                    STRING,
    Card_Age                     INT,
    Transaction_Distance         DECIMAL(10,2),
    Authentication_Method        STRING,
    Risk_Score                   DECIMAL(5,4),
    Is_Weekend                   INT,
    Fraud_Label                  INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hdfs_target_dir}'
TBLPROPERTIES ("skip.header.line.count"="0");
