# ============================================================
# Unity Catalog Permissions
# Defines roles for engineers, analysts, admins
# ============================================================

# Example groups (map to Azure AD / SCIM synced groups)
variable "engineers_group" { default = "data_engineers" }
variable "analysts_group"  { default = "data_analysts" }
variable "admins_group"    { default = "data_admins" }

# Engineers: Full access on landing/raw/harmonize
resource "databricks_grants" "engineers_access" {
  schema = databricks_schema.raw.id
  grant {
    principal  = var.engineers_group
    privileges = ["SELECT", "MODIFY"]
  }
}

resource "databricks_grants" "engineers_access_harmonize" {
  schema = databricks_schema.harmonize.id
  grant {
    principal  = var.engineers_group
    privileges = ["SELECT", "MODIFY"]
  }
}

# Analysts: Read-only on refined schema
resource "databricks_grants" "analysts_access_refined" {
  schema = databricks_schema.refined.id
  grant {
    principal  = var.analysts_group
    privileges = ["SELECT"]
  }
}

# Admins: All privileges on catalog
resource "databricks_grants" "admins_access" {
  catalog = databricks_catalog.datalake.id
  grant {
    principal  = var.admins_group
    privileges = ["ALL PRIVILEGES"]
  }
}
