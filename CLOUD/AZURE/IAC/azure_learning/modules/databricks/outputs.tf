output "workspace_id" {
  value = azurerm_databricks_workspace.dbw.id
}

output "workspace_url" {
  value = "https://${azurerm_databricks_workspace.dbw.workspace_url}"
}

output "cluster_id" {
  value = databricks_cluster.transform_cluster.id
}
