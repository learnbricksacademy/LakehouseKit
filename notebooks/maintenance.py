from src.maintenance import safe_execute, optimize_and_zorder, apply_retention_policy, vacuum_table
from src.utils import load_config

# ---------------- Params ----------------
dbutils.widgets.text("mode", "all")
dbutils.widgets.text("config_file", "/Workspace/Repos/databricks-data-migration-framework/configs/settings.yaml")

mode = dbutils.widgets.get("mode")
config = load_config(dbutils.widgets.get("config_file"))

# Migration Logs Maintenance
if mode in ("all", "optimize"):
    safe_execute(spark, optimize_and_zorder, ["system.migration_logs", ["source_name", "run_time"]], "Optimize migration_logs", dbutils, config)

if mode in ("all", "retention"):
    safe_execute(spark, apply_retention_policy, ["system.migration_logs", "run_time", "180 DAYS"], "Retention migration_logs", dbutils, config)

if mode in ("all", "vacuum"):
    safe_execute(spark, vacuum_table, ["system.migration_logs", 168], "Vacuum migration_logs", dbutils, config)

# Schema Registry Maintenance
if mode in ("all", "optimize"):
    safe_execute(spark, optimize_and_zorder, ["system.schema_registry", ["source_name", "ingest_time"]], "Optimize schema_registry", dbutils, config)

if mode in ("all", "retention"):
    safe_execute(spark, apply_retention_policy, ["system.schema_registry", "ingest_time", "2 YEARS"], "Retention schema_registry", dbutils, config)

if mode in ("all", "vacuum"):
    safe_execute(spark, vacuum_table, ["system.schema_registry", 720], "Vacuum schema_registry", dbutils, config)
