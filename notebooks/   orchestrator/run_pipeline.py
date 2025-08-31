# Databricks notebook source
import uuid
from pyspark.sql import functions as F
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

dbutils.widgets.text("pipeline_stage", "all")  
# Options: all | landing | raw | harmonize | refined | raw_harmonize | harmonize_refined

landing_config_file = dbutils.widgets.get("landing_config")
raw_config_file = dbutils.widgets.get("raw_config")
harmonize_config_file = dbutils.widgets.get("harmonize_config")
refined_config_file = dbutils.widgets.get("refined_config")
pipeline_stage = dbutils.widgets.get("pipeline_stage")

print(f"🚀 Starting pipeline with stage: {pipeline_stage}")

# ---------------- Pipeline Run Logging ----------------
def log_pipeline_run(spark, run_id, stage, status, details=None, end_time=None):
    """
    Log pipeline run details into system.pipeline_runs
    """
    now = spark.sql("SELECT current_timestamp()").collect()[0][0]
    row = [(run_id, stage, status, start_time, end_time or now, details)]
    df = spark.createDataFrame(
        row,
        ["run_id", "stage", "status", "start_time", "end_time", "details"]
    )
    df.write.format("delta").mode("append").saveAsTable("system.pipeline_runs")

# Generate unique run_id
run_id = str(uuid.uuid4())
start_time = spark.sql("SELECT current_timestamp()").collect()[0][0]

try:
    # Log run start
    log_pipeline_run(spark, run_id, pipeline_stage, "running", details="{'message':'Pipeline started'}")

    # ---------------- Landing → Raw ----------------
    if pipeline_stage in ("all", "landing"):
        landing_config = load_config(landing_config_file)["landing_sources"]
        for source in landing_config:
            print(f"📂 Landing → Raw: {source['name']}")
            load_file_to_raw(spark, source)

    # ---------------- Other Raw Sources (SQL, API, Storage) ----------------
    if pipeline_stage in ("all", "raw", "raw_harmonize"):
        raw_config = load_config(raw_config_file)["sources"]
        for source in raw_config:
            print(f"📥 Raw ingestion: {source['name']} ({source['type']})")
            if source["type"] == "sql":
                ingest_sql_source(spark, source)
            elif source["type"] == "api":
                ingest_api_source(spark, source)
            elif source["type"] == "storage":
                ingest_storage_source(spark, source)

    # ---------------- Raw → Harmonize ----------------
    if pipeline_stage in ("all", "harmonize", "raw_harmonize", "harmonize_refined"):
        harmonize_config = load_config(harmonize_config_file)["transformations"]
        engine = TransformationEngine(spark, base_path="harmonize")
        for t in harmonize_config:
            print(f"🔄 Harmonize transformation: {t['name']}")
            engine.run(t)

    # ---------------- Harmonize → Refined ----------------
    if pipeline_stage in ("all", "refined", "harmonize_refined"):
        refined_config = load_config(refined_config_file)["consumption"]
        for job in refined_config:
            print(f"📊 Refined job: {job['name']}")
            apply_consumption(spark, job)

    # Log run success
    log_pipeline_run(
        spark, 
        run_id, 
        pipeline_stage, 
        "success", 
        details="{'message':'Pipeline stage completed successfully'}"
    )

    print(f"✅ Pipeline stage '{pipeline_stage}' completed successfully (Run ID: {run_id})")

except Exception as e:
    # Log run failure
    log_pipeline_run(
        spark, 
        run_id, 
        pipeline_stage, 
        "failed", 
        details=f"{{'error':'{str(e)}'}}"
    )
    print(f"❌ Pipeline stage '{pipeline_stage}' failed (Run ID: {run_id})")
    raise
