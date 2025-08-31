# ============================================================
# Unity Catalog Setup
# Creates Catalog + Schemas for Landing, Raw, Harmonize, Refined
# ============================================================

resource "databricks_catalog" "datalake" {
  name        = "datalake"
  comment     = "Main governed catalog for data lake"
  provider    = databricks
}

resource "databricks_schema" "landing" {
  catalog_name = databricks_catalog.datalake.name
  name         = "landing"
  comment      = "Landing schema (files as-is)"
}

resource "databricks_schema" "raw" {
  catalog_name = databricks_catalog.datalake.name
  name         = "raw"
  comment      = "Raw schema (structured Delta after ingestion)"
}

resource "databricks_schema" "harmonize" {
  catalog_name = databricks_catalog.datalake.name
  name         = "harmonize"
  comment      = "Harmonized schema (standardized data)"
}

resource "databricks_schema" "refined" {
  catalog_name = databricks_catalog.datalake.name
  name         = "refined"
  comment      = "Refined schema (business-ready consumption layer)"
}
