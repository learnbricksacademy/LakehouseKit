-- ==========================================================
-- SEED SCRIPT (OPTIONAL, FOR DEV/TEST)
-- Inserts dummy rows into system tables for dashboard testing
-- ==========================================================

-- Seed migration logs
INSERT INTO system.migration_logs (source_name, source_type, target_table, format, file_count, total_size_bytes, validation_results)
VALUES 
  ('policies_file', 'landing', 'raw.policies', 'csv', 10, 5242880, '{"status":"ok"}'),
  ('customers_api', 'api', 'raw.customers', NULL, NULL, NULL, '{"row_count": 500, "schema_drift": "first_run"}');

-- Seed schema registry
INSERT INTO system.schema_registry (source_name, schema_json, ingest_time)
VALUES
  ('policies_file', '{"policy_id":"int","customer_id":"int","premium_amount":"double"}', current_timestamp()),
  ('customers_api', '{"customer_id":"string","email":"string"}', current_timestamp());

-- Seed incremental tracker
INSERT INTO system.incremental_tracker (source_name, last_value, processed_at)
VALUES
  ('policies_harmonized', current_timestamp() - INTERVAL 1 DAY, current_timestamp());

-- Seed transformation validations
INSERT INTO system.transformation_validations (transformation_name, rule, invalid_count, action)
VALUES
  ('policies_harmonized', 'premium_amount >= 0', 0, 'fail'),
  ('customers_harmonized', 'customer_id IS NOT NULL', 5, 'drop');

-- Seed transformation lineage
INSERT INTO system.transformation_lineage (transformation_name, source_table, target_table, rules)
VALUES
  ('policies_harmonized', 'raw.policies', 'harmonize.policies_standardized', '{"rename_columns": {"PolicyID":"policy_id"}}'),
  ('customer360_view', 'harmonize.customers_cleaned', 'refined.customer360', '{"join": {"on":"customer_id"}}');
