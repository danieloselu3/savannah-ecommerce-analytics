# Savannah E-commerce Analytics Project

## Project Description
Savannah E-commerce Analytics is a project designed to provide insightful analytics and reporting for e-commerce platforms. The project leverages data engineering and cloud technologies to streamline data processing and visualization, enabling businesses to make data-driven decisions efficiently.

## Project walk through Video
[![Project Walk through Video](https://img.youtube.com/vi/-OURBKTr34I/maxresdefault.jpg)](https://youtu.be/-OURBKTr34I)

## Basic Requirements
Before setting up the environment, ensure you have the following:
- Python installed (version 3.6 or higher)
- A Google Cloud Platform (GCP) account
- Terraform installed

## Creating and Activating a Virtual Environment

### Mac and Linux
1. Open your terminal.
2. Navigate to your project directory.
3. Create a virtual environment by running:
   ```bash
   python3 -m venv venv
   ```
4. Activate the virtual environment:
   ```bash
   source venv/bin/activate
   ```

### Windows
1. Open Command Prompt or PowerShell.
2. Navigate to your project directory.
3. Create a virtual environment by running:
   ```bash
   python -m venv venv
   ```
4. Activate the virtual environment:
   ```bash
   venv\Scripts\activate
   ```

## Installing Modules from `requirements.txt`
After activating your virtual environment, install the required Python modules by running:
```bash
pip install -r requirements.txt
```

## Installing Terraform
To install Terraform, follow the instructions on the [HashiCorp Terraform installation guide](https://developer.hashicorp.com/terraform/install).

## Installing Google Cloud SDK and Activating It

### Linux
1. Update your package list:
   ```bash
   sudo apt update
   ```
2. Install required packages:
   ```bash
   sudo apt install -y apt-transport-https ca-certificates gnupg
   ```
3. Add the Google Cloud SDK distribution URI as a package source:
   ```bash
   echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
   ```
4. Import the Google Cloud public key:
   ```bash
   curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
   ```
5. Update and install the Google Cloud SDK:
   ```bash
   sudo apt update
   sudo apt install google-cloud-sdk
   ```
6. Initialize the SDK (optional):
   ```bash
   gcloud init
   ```
7. Verify the installation:
   ```bash
   gcloud version
   ```

## Using `gcloud auth application-default login`
To use your Google Cloud credentials, run the following command:
```bash
gcloud auth application-default login
```

## Creating a `terraform.tfvars` File
Create a `terraform.tfvars` file with the following contents:
```hcl
project_id    = "your-google-cloud-project-id"
region        = "us-central1"
admin_email   = "your-admin-email@example.com"
```

## Enabling Required Google Cloud APIs
Ensure the following Google Cloud APIs are enabled:
- Cloud Storage API
- BigQuery API
- Cloud Data Composer API

## Running Terraform Commands
1. Initialize Terraform:
   ```bash
   terraform init
   ```
2. Plan your Terraform configuration:
   ```bash
   terraform plan
   ```
3. Apply the Terraform configuration:
   ```bash
   terraform apply
   ```

## Cloud Composer File Upload Guide

## Get the Bucket URL
Get the Cloud Composer bucket URL using:
```bash
terraform output composer_bucket
```

The bucket URL will be in format: 
`gs://us-central1-ecommerce-airfl-d927de92-bucket`

## Upload Process

### 1. Create Scripts Directory
Create the scripts folder in the Composer bucket:
```bash
gsutil mkdir gs://[BUCKET_NAME]/dags/scripts
```

### 2. Upload Main DAG File
Copy the main DAG file to the root dags folder:
```bash
gsutil cp scripts/savannah-dag.py gs://[BUCKET_NAME]/dags/
```

### 3. Upload Supporting Scripts
Upload each script to the scripts folder:

Extract files:
```bash
gsutil cp scripts/extract/api-data-extraction.py gs://[BUCKET_NAME]/dags/scripts/
```

Load files:
```bash
gsutil cp scripts/load/cart-bq-loader.py gs://[BUCKET_NAME]/dags/scripts/
gsutil cp scripts/load/product-bq-loader.py gs://[BUCKET_NAME]/dags/scripts/
gsutil cp scripts/load/user-bq-loader.py gs://[BUCKET_NAME]/dags/scripts/
```

Transform file:
```bash
gsutil cp scripts/transform/json-tocsv-conversion.py gs://[BUCKET_NAME]/dags/scripts/
```

### 4. Verify Upload
Verify the files are correctly placed:
```bash
gsutil ls gs://[BUCKET_NAME]/dags/
gsutil ls gs://[BUCKET_NAME]/dags/scripts/
```

Expected structure:
```
dags/
├── savannah-dag.py
└── scripts/
    ├── api-data-extraction.py
    ├── cart-bq-loader.py
    ├── json-tocsv-conversion.py
    ├── product-bq-loader.py
    └── user-bq-loader.py
```
