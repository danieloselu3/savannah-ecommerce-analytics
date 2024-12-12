# savannah-ecommerce-analytics
A Data Engineering project for creating a data pipeline to ingest Ecommerce data Using Google Cloud Platform as our Cloud Provider.

## Prerequisites

1. Google Cloud SDK installed
2. Terraform installed (v1.0+)
3. Google Cloud Project created
4. Service Account with appropriate permissions

## Configuration Steps

1. Create a `terraform.tfvars` file with the following contents:

```hcl
project_id    = "your-google-cloud-project-id"
region        = "us-central1"
admin_email   = "your-admin-email@example.com"
```

2. Enable required Google Cloud APIs:
- Cloud Storage API
- BigQuery API
- Cloud Data Composer API

## Deployment

```bash
# Initialize Terraform
terraform init

# Review the planned changes
terraform plan

# Apply the configuration
terraform apply
```

## Architecture Components

- **Storage Layers**:
  - Raw Layer GCS Bucket
  - Cleanse Layer GCS Bucket
  - Serve Layer BigQuery Dataset

- **Data Processing and Orchestration**:
  - Cloud Data Composer Instance
  - BigQuery Dataset


## Post-Deployment

1. Configure Cloud Data Composer
2. Set up data transformation jobs
3. Configure authentication
