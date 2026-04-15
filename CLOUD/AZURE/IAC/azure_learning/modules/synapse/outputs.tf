output "serverless_endpoint" {
  value = data.azurerm_synapse_workspace.existing.connectivity_endpoints["sqlOnDemand"]
}

output "synapse_workspace_name" {
  value = data.azurerm_synapse_workspace.existing.name
}
