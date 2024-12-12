# Project Configuration
variable "project_id" {
  description = "The Google Cloud project ID"
  type        = string
}

variable "region" {
  description = "The Google Cloud region"
  type        = string
  default     = "us-central1"
}

# Identity and Access Variables
variable "admin_email" {
  description = "Email of the admin user for BigQuery dataset access"
  type        = string
}

#Storage Configuration
variable "raw_layer_lifecycle_days" {
  description = "Number of days before raw layer data is deleted"
  type        = number
  default     = 30
}

variable "composer_image_version" {
  description = "Image version for Cloud Composer environment"
  type        = string
  default     = "composer-3-airflow-2.7.3"
}