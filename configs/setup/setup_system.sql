-- ==========================================================
-- INITIAL SETUP FOR INGESTION + TRANSFORMATION FRAMEWORK
-- Creates schemas & system tables required by framework
-- Safe to re-run anytime (idempotent)
-- ==========================================================

-- Ensure system schema exists
CREATE SCHEMA IF NOT EXISTS system;

-- ==========================================================
-- INGESTION SYSTEM TABLES
-- ==========================================================

-- Ingestion logs (run-level results, includes landing file metadata)
CREATE TABLE IF NOT EXISTS system.migration_logs (
  source_name STRING,
  source_type STRING,          -- sql | api | storage | landing
  target_table STRING,
  format STRING,               -- csv, json, parquet, etc.
  file_count BIGINT,           -- number of files processed
  total_size_bytes BIGINT,     -- size of files ingested
  validation_results STRING,   -- optional JSON
  run_time TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

-- Schema registry (tracks schema versions for drift detection)
CREATE TABLE IF NOT EXISTS system.schema_registry (
  source_name STRING,
  schema_json STRING,
  ingest_time TIMESTAMP
)
USING DELTA;

-- ==========================================================
-- TRANSFORMATION SYSTEM TABLES
-- ==========================================================

-- Incremental tracker (last processed watermark for CDC/incremental)
CREATE TABLE IF NOT EXISTS system.incremental_tracker (
  source_name STRING,
  last_value TIMESTAMP,
  processed_at TIMESTAMP
)
USING DELTA;

-- Validation results (data quality checks applied during transformation)
CREATE TABLE IF NOT EXISTS system.transformation_validations (
  transformation_name STRING,
  rule STRING,
  invalid_count BIGINT,
  action STRING,
  run_time TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

-- Lineage tracking (Raw → Harmonize, Harmonize → Refined)
CREATE TABLE IF NOT EXISTS system.transformation_lineage (
  transformation_name STRING,
  source_table STRING,
  target_table STRING,
  rules STRING,
  run_time TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

-- ==========================================================
-- ORCHESTRATOR / PIPELINE SYSTEM TABLES
-- ==========================================================

-- Logs each orchestrator run (for audit + monitoring)
CREATE TABLE IF NOT EXISTS system.pipeline_runs (
  run_id STRING,                -- unique ID per orchestrator run
  stage STRING,                 -- all | landing | raw | harmonize | refined | raw_harmonize | harmonize_refined
  status STRING,                -- success | failed
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  details STRING                -- optional JSON (configs, error messages, counts, etc.)
)
USING DELTA;

