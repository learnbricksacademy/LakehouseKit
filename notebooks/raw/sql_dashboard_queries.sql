-- Dashboard Tile: Migration Overview (Recent Runs)
SELECT 
  source_name,
  source_type,
  target_table,
  run_time,
  validation_results
FROM system.migration_logs
ORDER BY run_time DESC;

-- Dashboard Tile: Validation Row Counts
SELECT 
  source_name,
  target_table,
  run_time,
  get_json_object(validation_results, '$.row_count') AS row_count
FROM system.migration_logs
ORDER BY run_time DESC;

-- Dashboard Tile: Schema Drift Detection
SELECT 
  source_name,
  run_time,
  get_json_object(validation_results, '$.schema_drift') AS schema_drift
FROM system.migration_logs
WHERE get_json_object(validation_results, '$.schema_drift') != 'first_run'
ORDER BY run_time DESC;

-- Dashboard Tile: Schema Drift History
SELECT 
  source_name,
  ingest_time,
  schema_json
FROM system.schema_registry
ORDER BY ingest_time DESC;
