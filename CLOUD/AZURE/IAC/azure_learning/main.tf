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
  }
  required_version = ">= 1.5.0"
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = true
    }
  }
  subscription_id = var.subscription_id
  tenant_id       = var.tenant_id
}

provider "azuread" {
  tenant_id = var.tenant_id
}

provider "databricks" {
  azure_workspace_resource_id = module.databricks.workspace_id
}

# ─── Data Sources ───────────────────────────────────────────
data "azurerm_client_config" "current" {}

data "azurerm_resource_group" "itc_bigdata" {
  name = var.resource_group_name
}

# ─── Modules ────────────────────────────────────────────────

module "adls" {
  source              = "./modules/adls"
  resource_group_name = data.azurerm_resource_group.itc_bigdata.name
  location            = data.azurerm_resource_group.itc_bigdata.location
  project             = var.project
  environment         = var.environment
}

module "keyvault" {
  source              = "./modules/keyvault"
  resource_group_name = data.azurerm_resource_group.itc_bigdata.name
  location            = data.azurerm_resource_group.itc_bigdata.location
  project             = var.project
  environment         = var.environment
  tenant_id           = var.tenant_id
  current_object_id   = data.azurerm_client_config.current.object_id
  pg_host             = var.pg_host
  pg_port             = var.pg_port
  pg_database         = var.pg_database
  pg_username         = var.pg_username
  pg_password         = var.pg_password
  adls_account_key    = module.adls.storage_account_key
}

module "adf" {
  source                     = "./modules/adf"
  resource_group_name        = data.azurerm_resource_group.itc_bigdata.name
  location                   = data.azurerm_resource_group.itc_bigdata.location
  project                    = var.project
  environment                = var.environment
  adls_account_name          = module.adls.storage_account_name
  adls_account_key           = module.adls.storage_account_key
  raw_container_name         = module.adls.raw_container_name
  key_vault_id               = module.keyvault.key_vault_id
  key_vault_uri              = module.keyvault.key_vault_uri
  pg_host                    = var.pg_host
  pg_port                    = var.pg_port
  pg_database                = var.pg_database
  pg_username                = var.pg_username
  pg_password_secret_name    = module.keyvault.pg_password_secret_name
}

module "databricks" {
  source              = "./modules/databricks"
  resource_group_name = data.azurerm_resource_group.itc_bigdata.name
  location            = data.azurerm_resource_group.itc_bigdata.location
  project             = var.project
  environment         = var.environment
  adls_account_name   = module.adls.storage_account_name
  adls_account_key    = module.adls.storage_account_key
  raw_container_name  = module.adls.raw_container_name
  curated_container_name = module.adls.curated_container_name
}

module "synapse" {
  source                 = "./modules/synapse"
  resource_group_name    = data.azurerm_resource_group.itc_bigdata.name
  location               = data.azurerm_resource_group.itc_bigdata.location
  project                = var.project
  environment            = var.environment
  adls_account_name      = module.adls.storage_account_name
  adls_account_id        = module.adls.storage_account_id
  gold_container_name    = module.adls.gold_container_name
  adls_dfs_endpoint      = module.adls.dfs_endpoint
  existing_workspace_name = var.existing_synapse_workspace
  existing_workspace_rg   = var.resource_group_name
}
