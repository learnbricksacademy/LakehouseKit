## Usage
cd .devops/terraform
terraform init
terraform plan -var="databricks_host=..." -var="databricks_token=..." \
  -var="storage_account=..." -var="region=eastus" -var="workspace_id=..."
terraform apply


This will:

Create Unity Catalog & Metastore

Assign it to workspace

Apply catalog/schema permissions

Deploy jobs from your existing .devops/jobs.json
