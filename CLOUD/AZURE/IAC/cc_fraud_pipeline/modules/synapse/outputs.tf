output "serverless_endpoint" {
  value = data.azurerm_synapse_workspace.existing.connectivity_endpoints["sqlOnDemand"]
}

output "workspace_id" {
  value = data.azurerm_synapse_workspace.existing.id
}
