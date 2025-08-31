-- ==========================================================
-- SEED SCRIPT (optional for testing)
-- Inserts dummy rows into system tables for dashboard testing
-- ==========================================================

-- Switch to catalog if using Unity Catalog
-- USE CATALOG my_catalog;

-- Seed migration logs
INSERT INTO system.migration_logs (source_name, source_type, target_table, validation_results)
VALUES 
  ('policies', 'sql', 'raw.policies', '{"row_count": 1000, "schema_drift": "none"}'),
  ('customers_api', 'api', 'raw.customers', '{"row_count": 500, "schema_drift": "first_run"}');

-- Seed schema registry
INSERT INTO system.schema_registry (source_name, schema_json, ingest_time)
VALUES
  ('policies', '{"policy_id":"int","customer_id":"int","premium_amount":"double"}', current_timestamp()),
  ('customers_api', '{"customer_id":"string","email":"string"}', current_timestamp());

-- Seed incremental tracker
INSERT INTO system.incremental_tracker (source_name, last_value, processed_at)
VALUES
  ('policies_curated', current_timestamp() - INTERVAL 1 DAY, current_timestamp());

-- Seed transformation validations
INSERT INTO system.transformation_validations (transformation_name, rule, invalid_count, action)
VALUES
  ('policies_curated', 'premium_amount >= 0', 0, 'fail'),
  ('customers_curated', 'customer_id IS NOT NULL', 5, 'drop');

-- Seed transformation lineage
INSERT INTO system.transformation_lineage (transformation_name, source_table, target_table, rules)
VALUES
  ('policies_curated', 'raw.policies', 'curated.policies', '{"rename_columns": {"PolicyID":"policy_id"}}'),
  ('customers_curated', 'raw.customers', 'curated.customers', '{"drop_duplicates": {"subset":["customer_id"]}}');
