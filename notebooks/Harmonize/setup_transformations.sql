-- System schema
CREATE SCHEMA IF NOT EXISTS system;

-- Incremental tracker
CREATE TABLE IF NOT EXISTS system.incremental_tracker (
  source_name STRING,
  last_value TIMESTAMP,
  processed_at TIMESTAMP
)
USING DELTA;

-- Validation results
CREATE TABLE IF NOT EXISTS system.transformation_validations (
  transformation_name STRING,
  rule STRING,
  invalid_count BIGINT,
  action STRING,
  run_time TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

-- Lineage logs
CREATE TABLE IF NOT EXISTS system.transformation_lineage (
  transformation_name STRING,
  source_table STRING,
  target_table STRING,
  rules STRING,
  run_time TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;
