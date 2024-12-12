# Provider Configuration
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.80.0"
    }
  }
}

# Create a service account for the ecommerce data pipeline
resource "google_service_account" "ecommerce_data_pipeline" {
  account_id   = "ecommerce-data-pipeline"
  display_name = "Ecommerce Data Pipeline Service Account"
  description  = "Service account for managing ecommerce data pipeline resources"
}

# Google Cloud provider configuration
provider "google" {
  project = var.project_id
  region  = var.region
}

# Cloud Storage Bucket for Data Layers
resource "google_storage_bucket" "data_layers" {
  name          = "${var.project_id}-data-layers"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition {
      age = var.raw_layer_lifecycle_days
      matches_prefix = ["raw/"]
    }
    action {
      type = "Delete"
    }
  }
}

# IAM permissions for Cloud Storage
resource "google_storage_bucket_iam_member" "data_layers_admin" {
  bucket = google_storage_bucket.data_layers.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${google_service_account.ecommerce_data_pipeline.email}"
}

# Placeholder objects to create folder-like structure in GCS
resource "google_storage_bucket_object" "raw_folder" {
  name    = "raw/"
  content = "Placeholder for raw data layer"
  bucket  = google_storage_bucket.data_layers.name
}

resource "google_storage_bucket_object" "cleanse_folder" {
  name    = "cleanse/"
  content = "Placeholder for cleansed data layer"
  bucket  = google_storage_bucket.data_layers.name
}

# BigQuery Dataset
resource "google_bigquery_dataset" "ecommerce_dataset" {
  dataset_id                 = "ecommerce_data"
  friendly_name              = "Ecommerce Data Warehouse"
  description                = "Dataset for storing processed ecommerce data"
  location                   = var.region

  access {
    role          = "OWNER"
    user_by_email = var.admin_email
  }
}

# IAM permissions for BigQuery
resource "google_bigquery_dataset_iam_member" "ecommerce_dataset_admin" {
  dataset_id = google_bigquery_dataset.ecommerce_dataset.dataset_id
  role       = "roles/bigquery.admin"
  member     = "serviceAccount:${google_service_account.ecommerce_data_pipeline.email}"
}

# Cloud Composer Environment
resource "google_composer_environment" "ecommerce_airflow" {
  name    = "ecommerce-airflow-environment"
  region  = var.region
  config {
    software_config {
      image_version = var.composer_image_version
    }
  }
}

# IAM permissions for Cloud Composer
resource "google_project_iam_member" "composer_admin" {
  project = var.project_id
  role    = "roles/composer.admin"
  member  = "serviceAccount:${google_service_account.ecommerce_data_pipeline.email}"
}


