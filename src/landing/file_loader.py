from pyspark.sql import functions as F
from src.landing.validator import validate_files

def load_file_to_raw(spark, config, base_path="landing"):
    """
    Load files from Landing layer into Raw Delta tables
    - Runs landing-level validations
    - Enriches with ingestion timestamp
    - Logs into system.migration_logs
    """

    path = config["path"]
    fmt = config["format"]
    raw_table = config["raw_table"]
    options = config.get("options", {})
    mode = config.get("mode", "append")
    source_name = config["name"]

    # --- Run validations (returns DataFrame if passes) ---
    df = validate_files(spark, config)

    # --- Add ingestion timestamp ---
    df = df.withColumn("_ingest_time", F.current_timestamp())

    # --- Write to Raw Delta table ---
    df.write.format("delta").mode(mode).saveAsTable(raw_table)

    # --- Gather file stats ---
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        path_obj = spark._jvm.org.apache.hadoop.fs.Path(path)
        statuses = fs.listStatus(path_obj)

        file_count = len(statuses)
        total_size = sum([s.getLen() for s in statuses])
    except Exception as e:
        print(f"⚠️ Could not fetch file stats for {path}: {e}")
        file_count, total_size = None, None

    # --- Prepare log record ---
    log_data = [(source_name, "landing", raw_table, fmt, file_count, total_size, None)]
    log_df = spark.createDataFrame(
        log_data,
        ["source_name", "source_type", "target_table", "format", "file_count", "total_size_bytes", "validation_results"]
    )

    # --- Write log record ---
    log_df.write.format("delta").mode("append").saveAsTable("system.migration_logs")

    print(f"✅ Loaded {file_count or 0} {fmt.upper()} files from {path} → {raw_table} "
          f"({total_size/1024/1024 if total_size else 0:.2f} MB) in {mode} mode")

    return df
