# How to Use
Terraform

Add provider config (provider "databricks" {}) in your Terraform root.

Run:

terraform init
terraform plan
terraform apply

Jobs JSON (via CLI or CI/CD)
databricks jobs create --json-file .devops/jobs.json
# or update
databricks jobs reset --job-id <job_id> --json-file .devops/jobs.json

🔹 Benefits

✔️ Unity Catalog for governance
✔️ Role-based access for engineers, analysts, admins
✔️ Jobs defined as code (reproducible)
✔️ CI/CD ready (jobs.json works with GitHub Actions/Azure DevOps)
