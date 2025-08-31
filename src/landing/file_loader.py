import os
from pyspark.sql import functions as F

def load_file_to_raw(spark, config, base_path):
    path = config["path"]
    fmt = config["format"]
    raw_table = config["raw_table"]
    options = config.get("options", {})
    mode = config.get("mode", "append")
    source_name = config["name"]

    # Read files
    df = spark.read.format(fmt).options(**options).load(path)

    # Add ingestion timestamp
    df = df.withColumn("_ingest_time", F.current_timestamp())

    # Write to raw table
    df.write.format("delta").mode(mode).saveAsTable(raw_table)

    # --- Gather file stats ---
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        uri = spark._jvm.java.net.URI(path)
        path_obj = spark._jvm.org.apache.hadoop.fs.Path(path)
        status_list = fs.listStatus(path_obj)

        file_count = len(status_list)
        total_size = sum([s.getLen() for s in status_list])
    except Exception as e:
        print(f"⚠️ Could not fetch file stats for {path}: {e}")
        file_count, total_size = None, None

    # --- Log into migration_logs ---
    log_df = spark.createDataFrame(
        [(source_name, "landing", raw_table, fmt, file_count, total_size, None)],
        ["source_name", "source_type", "target_table", "format", "file_count", "total_size_bytes", "validation_results"]
    )
    log_df.write.format("delta").mode("append").saveAsTable("system.migration_logs")

    print(f"✅ File from {path} loaded into {raw_table} in {mode} mode ({file_count} files, {total_size} bytes)")
    return df
