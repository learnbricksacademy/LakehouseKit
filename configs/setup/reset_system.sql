-- ==========================================================
-- RESET SCRIPT (ONLY FOR DEV/TEST USE!)
-- Drops all system tables created by the framework
-- ==========================================================

DROP TABLE IF EXISTS system.migration_logs;
DROP TABLE IF EXISTS system.schema_registry;
DROP TABLE IF EXISTS system.incremental_tracker;
DROP TABLE IF EXISTS system.transformation_validations;
DROP TABLE IF EXISTS system.transformation_lineage;

-- Optionally drop schema if empty
DROP SCHEMA IF EXISTS system;
