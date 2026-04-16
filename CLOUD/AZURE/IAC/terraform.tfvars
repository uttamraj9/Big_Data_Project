subscription_id       = "3a72be92-287b-4f1e-840a-5e3e71100139"
tenant_id             = "2b32b1fa-7899-482e-a6de-be99c0ff5516"
location              = "northeurope"
resource_group_name   = "Itc_Bigdata"

adls_account_name         = "itcbdneadls"
adf_name                  = "itc-bd-ne-adf"
key_vault_name            = "itc-bd-ne-kv"
databricks_workspace_name = "itc-bd-ne-adb"
synapse_workspace_name    = "itc-bd-ne-synapse"

synapse_sql_admin    = "sqladmin"
synapse_sql_password = "Itc@Bigdata2024!"

pg_host     = "13.42.152.118"
pg_port     = 5432
pg_database = "testdb"
pg_username = "admin"
pg_password = "admin123"

# ─── Synapse Studio access ────────────────────────────────────
# Add a new group here (display_name = object_id) to grant
# Synapse Contributor + ADLS Reader — then run: terraform apply
studio_access_groups = {
  "ITC_BD_Group_FE" = "0b22faf8-f328-4fa4-b2e6-1d0728283eee"
}

# ─── Key Vault Secrets Officer ────────────────────────────────
# Add a new group here to grant create/read/update/delete on KV secrets
kv_secrets_officer_groups = {
  "ITC_BD_Group_FE" = "0b22faf8-f328-4fa4-b2e6-1d0728283eee"
}
