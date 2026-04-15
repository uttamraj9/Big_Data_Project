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
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.36"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
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

# Databricks provider points to the existing workspace
provider "databricks" {
  azure_workspace_resource_id = var.databricks_workspace_resource_id
}

# ─── Data Sources ────────────────────────────────────────────
data "azurerm_client_config" "current" {}

data "azurerm_resource_group" "itc_bigdata" {
  name = var.resource_group_name
}

# ─── Modules ─────────────────────────────────────────────────

module "adls" {
  source              = "./modules/adls"
  resource_group_name = data.azurerm_resource_group.itc_bigdata.name
  adls_account_name   = var.adls_account_name
}

module "keyvault" {
  source            = "./modules/keyvault"
  resource_group_name = data.azurerm_resource_group.itc_bigdata.name
  key_vault_name    = var.key_vault_name
  current_object_id = data.azurerm_client_config.current.object_id
  tenant_id         = var.tenant_id
  pg_host           = var.pg_host
  pg_port           = var.pg_port
  pg_database       = var.pg_database
  pg_username       = var.pg_username
  pg_password       = var.pg_password
  adls_account_key  = module.adls.storage_account_key
}

module "adf" {
  source                  = "./modules/adf"
  resource_group_name     = data.azurerm_resource_group.itc_bigdata.name
  adf_name                = var.adf_name
  adls_account_name       = module.adls.storage_account_name
  adls_account_key        = module.adls.storage_account_key
  raw_container_name      = module.adls.raw_container_name
  key_vault_id            = module.keyvault.key_vault_id
  key_vault_uri           = module.keyvault.key_vault_uri
  pg_host                 = var.pg_host
  pg_port                 = var.pg_port
  pg_database             = var.pg_database
  pg_username             = var.pg_username
  pg_password_secret_name = module.keyvault.pg_password_secret_name

  depends_on = [module.keyvault]
}

module "databricks" {
  source                 = "./modules/databricks"
  databricks_workspace_name = var.databricks_workspace_name
  resource_group_name    = data.azurerm_resource_group.itc_bigdata.name
  adls_account_name      = module.adls.storage_account_name
  raw_container_name     = module.adls.raw_container_name
  curated_container_name = module.adls.curated_container_name
  gold_container_name    = module.adls.gold_container_name
}

module "synapse" {
  source                  = "./modules/synapse"
  resource_group_name     = data.azurerm_resource_group.itc_bigdata.name
  synapse_workspace_name  = var.synapse_workspace_name
  adls_account_name       = module.adls.storage_account_name
  adls_account_id         = module.adls.storage_account_id
}
