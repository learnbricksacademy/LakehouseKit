# Databricks notebook source
from src.utils import load_config
from src.ingest_sql import ingest_sql
from src.ingest_api import ingest_api
from src.ingest_storage import ingest_storage
from src.validator import run_validation
from src.writer import write_to_delta

# ---------------- Params ----------------
dbutils.widgets.text("source_name", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container", "")
dbutils.widgets.text("config_file", "/Workspace/Repos/databricks-data-migration-framework/configs/multi_sources.yaml")

source_name = dbutils.widgets.get("source_name")
storage_account = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")
config_file = dbutils.widgets.get("config_file")

base_path = f"{container}@{storage_account}.dfs.core.windows.net"

# ---------------- Load Config ----------------
config = load_config(config_file)
sources = config["sources"]
source = next((s for s in sources if s["name"] == source_name), None)

if not source:
    raise Exception(f"Source {source_name} not found in config")

print(f"🚀 Starting ingestion for {source_name} ({source['type']})")

# ---------------- Ingest ----------------
if source["type"] == "sql":
    df = ingest_sql(spark, source, dbutils)
elif source["type"] == "api":
    df = ingest_api(spark, source, dbutils)
elif source["type"] == "storage":
    df = ingest_storage(spark, source, base_path)
else:
    raise ValueError(f"Unsupported source type: {source['type']}")

print(f"✅ Loaded {df.count()} records")

# ---------------- Write ----------------
target = source["target"]
streaming_cfg = source.get("streaming", {})
table_name = write_to_delta(df, target, base_path, streaming_cfg)

print(f"✅ Data written to {table_name}")

# ---------------- Validate ----------------
validation = source.get("validation", {})
results = run_validation(df, validation, source, spark)

print(f"🔎 Validation Results: {results}")

# ---------------- Log ----------------
log_df = spark.createDataFrame(
    [(source['name'], source['type'], table_name, str(results))],
    ["source_name", "source_type", "target_table", "validation_results"]
)

log_df.write.format("delta").mode("append").saveAsTable("system.migration_logs")
print("✅ Logged results into system.migration_logs")
