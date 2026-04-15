output "workspace_url" {
  value = data.azurerm_databricks_workspace.dbw.workspace_url
}

output "cluster_id" {
  value = databricks_cluster.fraud_transform.id
}
