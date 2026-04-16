"""
init_watermark.py
=================
One-time initialisation notebook — run this ONCE before the first incremental
pipeline execution to create the watermark file in ADLS.

Sets last_watermark to '2000-01-01 00:00:00' so the first ADF run copies ALL
existing data from PostgreSQL.

After each successful Databricks run, raw_to_curated.py will advance the
watermark to the max(timestamp) seen in the batch.
"""

import json

# ─── Widget Parameters ────────────────────────────────────────
dbutils.widgets.text("adls_account_name", "itcbdneadls")
dbutils.widgets.text("raw_container",     "raw")
dbutils.widgets.text("table_name",        "cc_fraud_trans")

ADLS_ACCOUNT  = dbutils.widgets.get("adls_account_name")
RAW_CONTAINER = dbutils.widgets.get("raw_container")
TABLE         = dbutils.widgets.get("table_name")

# ─── ADLS Access ─────────────────────────────────────────────
adls_key = dbutils.secrets.get(scope="adls-scope", key="adls-account-key")
spark.conf.set(
    f"fs.azure.account.key.{ADLS_ACCOUNT}.dfs.core.windows.net",
    adls_key
)

# ─── Write initial watermark ─────────────────────────────────
WATERMARK_PATH = (
    f"abfss://{RAW_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net"
    f"/watermark/{TABLE}.json"
)

initial_watermark = json.dumps({"last_watermark": "2000-01-01 00:00:00"})
dbutils.fs.put(WATERMARK_PATH, initial_watermark, overwrite=True)

print(f"[INFO] Watermark initialised at: {WATERMARK_PATH}")
print(f"[INFO] Value: {initial_watermark}")
