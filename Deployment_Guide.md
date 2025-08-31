# 📘 Deployment Guide

This document provides a **step-by-step playbook** for deploying and testing the data platform when a new company adopts this product.

---

## 1. Environment Setup

**Files / Notebooks:**

* `configs/env/dev.yaml`, `test.yaml`, `prod.yaml`
* `configs/setup/setup_system.sql`, `seed_system.sql`, `reset_system.sql`

**Steps:**

1. Clone repo into company Git.
2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```
3. Configure secrets (Databricks, storage, key vault) in CI/CD platform.
4. Run `setup_system.sql` to initialize metadata DB.
5. Apply `seed_system.sql` for test/demo data.

**Validation:**

* Run `tests/test_data_setup.sql`
* Run `pytest tests/test_ingestion.py`

---

## 2. Governance & Catalogs

**Files / Notebooks:**

* `.devops/terraform/unity_catalog.tf`, `permissions.tf`, `jobs.tf`
* `.devops/terraform/variables.tf`, `outputs.tf`
* `configs/governance/permissions.yaml`
* `notebooks/governance/apply_permissions.py`

**Steps:**

1. Deploy Unity Catalog, schemas, and jobs using Terraform:

   ```bash
   cd .devops/terraform
   terraform init
   terraform plan \
     -var="databricks_host=..." \
     -var="databricks_token=..." \
     -var="storage_account=..." \
     -var="region=eastus" \
     -var="workspace_id=..."
   terraform apply
   ```
2. Apply permissions from `permissions.yaml`:

   ```bash
   databricks notebooks run notebooks/governance/apply_permissions.py
   ```

**Validation:**

* Run `pytest tests/test_refined.py`

---

## 3. Landing Layer (Raw Staging)

**Files / Notebooks:**

* `configs/landing/landing_sources.yaml`
* `notebooks/landing/ingest_files.py`
* `src/landing/file_loader.py`, `validator.py`

**Steps:**

1. Define sources in `landing_sources.yaml`.
2. Ingest data:

   ```bash
   databricks notebooks run notebooks/landing/ingest_files.py
   ```

**Validation:**

* Run `pytest tests/test_ingestion.py`

---

## 4. Raw Layer (Standardized Ingestion)

**Files / Notebooks:**

* `configs/raw/multi_sources.yaml`, `settings.yaml`
* `notebooks/raw/ingest.py`
* `src/raw/ingest_api.py`, `ingest_sql.py`, `ingest_storage.py`

**Steps:**

1. Configure ingestion in `multi_sources.yaml`.
2. Run:

   ```bash
   databricks notebooks run notebooks/raw/ingest.py
   ```

**Validation:**

* Run `pytest tests/test_ingestion.py`
* Dashboard: `dashboards/raw/migration_dashboard.json`

---

## 5. Harmonize Layer (Transform + DQ)

**Files / Notebooks:**

* `configs/harmonize/dq_expectations.yaml`, `transformations.yaml`
* `notebooks/harmonize/run_transformations.py`, `run_dq_checks.py`
* `src/harmonize/base_engine.py`, `dq_engine.py`

**Steps:**

1. Define transformations and DQ rules.
2. Run transformations:

   ```bash
   databricks notebooks run notebooks/harmonize/run_transformations.py
   ```
3. Run DQ checks:

   ```bash
   databricks notebooks run notebooks/harmonize/run_dq_checks.py
   ```

**Validation:**

* Run `pytest tests/test_transformations.py`
* Run `pytest tests/test_data_quality.py`
* Dashboard: `dashboards/harmonize/transformation_dashboard.json`

---

## 6. Refined Layer (Consumption + Views)

**Files / Notebooks:**

* `configs/refined/consumption.yaml`, `views.yaml`
* `notebooks/refined/run_consumption.py`, `create_views.py`
* `src/refined/consumption.py`

**Steps:**

1. Define consumption rules + views.
2. Run consumption pipeline:

   ```bash
   databricks notebooks run notebooks/refined/run_consumption.py
   ```
3. Create views:

   ```bash
   databricks notebooks run notebooks/refined/create_views.py
   ```

**Validation:**

* Run `pytest tests/test_refined.py`
* Dashboard: `dashboards/refined/refined_health.json`

---

## 7. Orchestration (Full Pipeline)

**Files / Notebooks:**

* `notebooks/orchestrator/run_pipeline.py`
* `dashboards/orchestrator/pipeline_health.json`

**Steps:**

1. Trigger orchestrator:

   ```bash
   databricks notebooks run notebooks/orchestrator/run_pipeline.py
   ```

**Validation:**

* Run `pytest tests/test_e2e_pipeline.py`
* Dashboard: `dashboards/orchestrator/pipeline_health.json`

---

## 8. Alerts & Monitoring

**Files:**

* `alerts/alerts.json`
* `src/raw/notifier.py`

**Steps:**

1. Configure alert rules.
2. Simulate a failure to validate alerting.

**Validation:**

* Ensure notifier triggers correctly.

---

## 9. CI/CD Implementation

**Files:**

* `.devops/azure-pipelines.yaml`
* `.github/workflows/databricks_cicd.yaml`
* `.devops/jobs.json`
* `.devops/terraform/*`

**Steps:**

1. Configure pipeline in Azure DevOps or GitHub.
2. CI pipeline stages:

   * Lint + style check (`flake8`, `black`).
   * Run unit + integration tests.
   * Run `terraform validate` + `terraform plan`.
3. CD stages:

   * Deploy Terraform (Unity Catalog, jobs, permissions).
   * Deploy jobs from `jobs.json`.
   * Deploy notebooks to Databricks.

**Validation:**

* Push code → confirm pipeline runs end-to-end.

---

# 🏁 Final Flow

1. Setup environment
2. Apply governance (Terraform + permissions)
3. Ingest Landing → Raw → Harmonize → Refined
4. Run orchestrator
5. Verify dashboards + alerts
6. Enable CI/CD (with Terraform deployment)

This ensures a **fully functional, governed, monitored, and automated** deployment for new companies adopting the platform.
