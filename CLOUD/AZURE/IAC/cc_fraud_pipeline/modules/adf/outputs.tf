output "adf_name" {
  value = azurerm_data_factory.adf.name
}

output "adf_id" {
  value = azurerm_data_factory.adf.id
}

output "pipeline_name" {
  value = azurerm_data_factory_pipeline.pg_to_raw.name
}
