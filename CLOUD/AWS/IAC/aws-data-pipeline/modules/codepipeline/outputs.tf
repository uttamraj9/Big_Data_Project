output "pipeline_name"     { value = aws_codepipeline.glue_pipeline.name }
output "repo_clone_url"    { value = aws_codecommit_repository.glue_scripts.clone_url_http }
