from pyspark.sql import SparkSession

def load_file_to_raw(spark, config, base_path):
    path = config["path"]
    fmt = config["format"]
    raw_table = config["raw_table"]
    options = config.get("options", {})
    mode = config.get("mode", "append")

    df = spark.read.format(fmt).options(**options).load(path)

    # Add ingestion timestamp
    df = df.withColumn("_ingest_time", F.current_timestamp())

    # Write to raw table
    df.write.format("delta").mode(mode).saveAsTable(raw_table)

    print(f"✅ File from {path} loaded into {raw_table} in {mode} mode")
    return df
