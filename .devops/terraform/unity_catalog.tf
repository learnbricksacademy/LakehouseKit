terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.30.0"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

resource "databricks_metastore" "this" {
  name          = "company_metastore"
  storage_root  = "abfss://metastore@${var.storage_account}.dfs.core.windows.net/"
  region        = var.region
}

resource "databricks_metastore_assignment" "default" {
  workspace_id = var.workspace_id
  metastore_id = databricks_metastore.this.id
  default_catalog_name = "main"
}
