output "adf_name" {
  value = azurerm_data_factory.adf.name
}

output "adf_id" {
  value = azurerm_data_factory.adf.id
}

output "adf_principal_id" {
  value = azurerm_data_factory.adf.identity[0].principal_id
}
