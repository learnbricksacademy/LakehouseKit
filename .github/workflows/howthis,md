# How This Works

Branch → Environment Mapping

dev → deploys notebooks only (no Terraform apply)

test → deploys notebooks + Terraform apply

main → full deploy to PROD

Terraform manages Unity Catalog, schemas, permissions, job infra.

Databricks CLI imports notebooks + applies jobs.json.

## Required GitHub Secrets

Set these in your repo → Settings → Secrets and variables → Actions:

DATABRICKS_HOST → your workspace URL (https://adb-xxxx.azuredatabricks.net)

DATABRICKS_TOKEN → PAT (personal access token or service principal token)

ORCHESTRATOR_JOB_ID → job ID for your orchestrator (if exists; otherwise it will create one)

## Benefits

✔️ Full CI/CD automation for your framework
✔️ Environment aware (Dev → Test → Prod)
✔️ Infra as code (Terraform) + Job as code (jobs.json)
✔️ No manual steps once set up
