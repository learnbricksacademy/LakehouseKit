# ======================================================
# Makefile for CFP Data Platform
# Provides shortcuts for common dev/test/deploy actions
# ======================================================

# Default environment (can override with: make test ENV=test)
ENV ?= dev

# Paths
CONFIGS = configs/env/$(ENV).yaml
REQ = requirements.txt

# --- Setup & Install ---
install:
	@echo "Installing dependencies..."
	pip install -r $(REQ)

lint:
	@echo "Running lint checks..."
	flake8 src tests utils

format:
	@echo "Auto-formatting code..."
	black src tests utils

# --- Testing ---
test:
	@echo "Running unit and integration tests with env=$(ENV)..."
	pytest --cov=src --maxfail=1 -q

test-e2e:
	@echo "Running end-to-end pipeline smoke test..."
	pytest tests/test_e2e_pipeline.py -q

# --- Terraform / Infra ---
terraform-init:
	cd .devops/terraform && terraform init

terraform-plan:
	cd .devops/terraform && terraform plan -var-file=$(ENV).tfvars

terraform-apply:
	cd .devops/terraform && terraform apply -var-file=$(ENV).tfvars -auto-approve

# --- Databricks CLI / Deployment ---
db-login:
	@echo "Logging into Databricks workspace for env=$(ENV)..."
	databricks configure --token

deploy-jobs:
	@echo "Deploying Databricks jobs for env=$(ENV)..."
	databricks jobs create --json-file .devops/jobs.json

# --- Docs ---
docs:
	@echo "Generating docs (ADR index + README refresh)..."
	@ls docs/adr > docs/adr/index.txt
	@echo "ADR index updated at docs/adr/index.txt"

# --- Cleanup ---
clean:
	@echo "Cleaning up build artifacts..."
	rm -rf .pytest_cache .mypy_cache .coverage dist build

# --- Meta ---
help:
	@echo "Available commands:"
	@echo "  install          - Install Python dependencies"
	@echo "  lint             - Run flake8 lint checks"
	@echo "  format           - Format code with black"
	@echo "  test             - Run all tests (unit + integration)"
	@echo "  test-e2e         - Run end-to-end pipeline test"
	@echo "  terraform-init   - Init Terraform"
	@echo "  terraform-plan   - Terraform plan with env vars"
	@echo "  terraform-apply  - Apply Terraform changes"
	@echo "  db-login         - Configure Databricks CLI"
	@echo "  deploy-jobs      - Deploy Databricks jobs"
	@echo "  docs             - Update ADR index"
	@echo "  clean            - Remove build/test artifacts"
