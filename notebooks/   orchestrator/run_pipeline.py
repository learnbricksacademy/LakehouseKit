# Databricks notebook source
from src.utils import load_config
from src.landing.file_loader import load_file_to_raw
from src.raw.ingest_sql import ingest_sql_source
from src.raw.ingest_api import ingest_api_source
from src.raw.ingest_storage import ingest_storage_source
from src.harmonize.base_engine import TransformationEngine
from src.refined.consumption import apply_consumption

# ---------------- Params ----------------
dbutils.widgets.text("landing_config", "/Workspace/Repos/databricks-data-migration-framework/configs/landing/landing_sources.yaml")
dbutils.widgets.text("raw_config", "/Workspace/Repos/databricks-data-migration-framework/configs/raw/multi_sources.yaml")
dbutils.widgets.text("harmonize_config", "/Workspace/Repos/databricks-data-migration-framework/configs/harmonize/transformations.yaml")
dbutils.widgets.text("refined_config", "/Workspace/Repos/databricks-data-migration-framework/configs/refined/consumption.yaml")

landing_config_file = dbutils.widgets.get("landing_config")
raw_config_file = dbutils.widgets.get("raw_config")
harmonize_config_file = dbutils.widgets.get("harmonize_config")
refined_config_file = dbutils.widgets.get("refined_config")

# ---------------- Landing → Raw ----------------
landing_config = load_config(landing_config_file)["landing_sources"]
for source in landing_config:
    print(f"🚀 Landing → Raw: {source['name']}")
    load_file_to_raw(spark, source)

# ---------------- Other Raw Sources (SQL, API, Storage) ----------------
raw_config = load_config(raw_config_file)["sources"]
for source in raw_config:
    print(f"🚀 Raw ingestion: {source['name']} ({source['type']})")
    if source["type"] == "sql":
        ingest_sql_source(spark, source)
    elif source["type"] == "api":
        ingest_api_source(spark, source)
    elif source["type"] == "storage":
        ingest_storage_source(spark, source)

# ---------------- Raw → Harmonize ----------------
harmonize_config = load_config(harmonize_config_file)["transformations"]
engine = TransformationEngine(spark, base_path="harmonize")

for t in harmonize_config:
    print(f"🚀 Harmonize transformation: {t['name']}")
    engine.run(t)

# ---------------- Harmonize → Refined ----------------
refined_config = load_config(refined_config_file)["consumption"]
for job in refined_config:
    print(f"🚀 Refined job: {job['name']}")
    apply_consumption(spark, job)

print("✅ End-to-end pipeline completed successfully")
