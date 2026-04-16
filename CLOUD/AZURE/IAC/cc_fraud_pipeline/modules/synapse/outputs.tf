output "serverless_endpoint" {
  value = azurerm_synapse_workspace.synapse.connectivity_endpoints["sqlOnDemand"]
}

output "workspace_id" {
  value = azurerm_synapse_workspace.synapse.id
}
