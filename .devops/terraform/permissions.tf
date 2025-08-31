resource "databricks_grants" "catalog_permissions" {
  catalog = "main"

  grant {
    principal  = "data_engineers"
    privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
  }

  grant {
    principal  = "data_scientists"
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "schema_permissions" {
  schema = "main.raw"

  grant {
    principal  = "data_engineers"
    privileges = ["SELECT", "MODIFY"]
  }
}
