# GCS Data Layers Bucket Outputs
output "data_layers_bucket_name" {
  description = "Name of the data layers storage bucket"
  value       = google_storage_bucket.data_layers.name
}

output "data_layers_bucket_url" {
  description = "URL of the data layers storage bucket"
  value       = google_storage_bucket.data_layers.url
}

output "raw_layer_path" {
  description = "Path to the raw data layer in the bucket"
  value       = "gs://${google_storage_bucket.data_layers.name}/raw/"
}

output "cleanse_layer_path" {
  description = "Path to the cleansed data layer in the bucket"
  value       = "gs://${google_storage_bucket.data_layers.name}/cleanse/"
}

# BigQuery Outputs
output "bigquery_dataset_id" {
  description = "ID of the BigQuery dataset for ecommerce data"
  value       = google_bigquery_dataset.ecommerce_dataset.dataset_id
}

output "bigquery_dataset_self_link" {
  description = "Self link of the BigQuery dataset"
  value       = google_bigquery_dataset.ecommerce_dataset.self_link
}

# Cloud Composer Outputs
output "composer_environment_name" {
  description = "Name of the Cloud Composer environment"
  value       = google_composer_environment.ecommerce_airflow.name
}

output "composer_environment_web_server_url" {
  description = "Web server URL of the Cloud Composer environment"
  value       = google_composer_environment.ecommerce_airflow.config[0].airflow_uri
}

output "composer_environment_gke_cluster" {
  description = "GKE cluster used by the Cloud Composer environment"
  value       = google_composer_environment.ecommerce_airflow.config[0].gke_cluster
}