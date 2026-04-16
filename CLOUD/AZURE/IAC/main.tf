terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.47"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
  required_version = ">= 1.5.0"
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
  tenant_id       = var.tenant_id
}

provider "azuread" {
  tenant_id = var.tenant_id
}

# ─── Identity context ─────────────────────────────────────────
data "azurerm_client_config" "current" {}

# ─── Resource Group ───────────────────────────────────────────
resource "azurerm_resource_group" "itc_bigdata" {
  name     = var.resource_group_name
  location = var.location

  tags = {
    ManagedBy = "Terraform"
    Project   = "cc_fraud_pipeline"
  }
}

# ─── ITC_BD_Group_FE — Contributor on the RG ─────────────────
data "azuread_group" "itc_bd_group_fe" {
  display_name     = "ITC_BD_Group_FE"
  security_enabled = true
}

resource "azurerm_role_assignment" "itc_bd_group_fe_contributor" {
  scope                = azurerm_resource_group.itc_bigdata.id
  role_definition_name = "Contributor"
  principal_id         = data.azuread_group.itc_bd_group_fe.object_id
}

# ─── Modules ─────────────────────────────────────────────────

module "adls" {
  source              = "./modules/adls"
  resource_group_name = azurerm_resource_group.itc_bigdata.name
  location            = azurerm_resource_group.itc_bigdata.location
  adls_account_name   = var.adls_account_name
}

module "keyvault" {
  source              = "./modules/keyvault"
  resource_group_name = azurerm_resource_group.itc_bigdata.name
  location            = azurerm_resource_group.itc_bigdata.location
  key_vault_name      = var.key_vault_name
  current_object_id   = data.azurerm_client_config.current.object_id
  tenant_id           = var.tenant_id
  pg_host             = var.pg_host
  pg_port             = var.pg_port
  pg_database         = var.pg_database
  pg_username         = var.pg_username
  pg_password         = var.pg_password
  adls_account_key    = module.adls.storage_account_key

  depends_on = [module.adls]
}

module "adf" {
  source                  = "./modules/adf"
  resource_group_name     = azurerm_resource_group.itc_bigdata.name
  location                = azurerm_resource_group.itc_bigdata.location
  adf_name                = var.adf_name
  adls_account_name       = module.adls.storage_account_name
  adls_account_key        = module.adls.storage_account_key
  key_vault_id            = module.keyvault.key_vault_id
  pg_host                 = var.pg_host
  pg_port                 = var.pg_port
  pg_database             = var.pg_database
  pg_username             = var.pg_username
  pg_password_secret_name = module.keyvault.pg_password_secret_name

  depends_on = [module.keyvault]
}

module "databricks" {
  source                    = "./modules/databricks"
  resource_group_name       = azurerm_resource_group.itc_bigdata.name
  location                  = azurerm_resource_group.itc_bigdata.location
  databricks_workspace_name = var.databricks_workspace_name
  adls_account_name         = module.adls.storage_account_name
  raw_container_name        = module.adls.raw_container_name
  curated_container_name    = module.adls.curated_container_name
  gold_container_name       = module.adls.gold_container_name
}

module "synapse" {
  source                 = "./modules/synapse"
  resource_group_name    = azurerm_resource_group.itc_bigdata.name
  location               = azurerm_resource_group.itc_bigdata.location
  synapse_workspace_name = var.synapse_workspace_name
  adls_account_id        = module.adls.storage_account_id
  adls_filesystem_id     = module.adls.synapse_filesystem_id
  synapse_sql_admin      = var.synapse_sql_admin
  synapse_sql_password   = var.synapse_sql_password
  studio_access_groups   = var.studio_access_groups

  depends_on = [module.adls]
}
