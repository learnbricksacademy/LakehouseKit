import re
from pyspark.sql import functions as F

def validate_files(spark, config):
    """
    Run landing-level validations before loading into Raw
    """
    path = config["path"]
    fmt = config["format"]
    validations = config.get("validations", {})

    # --- File name pattern check ---
    if "file_name_pattern" in validations:
        pattern = validations["file_name_pattern"]
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        path_obj = spark._jvm.org.apache.hadoop.fs.Path(path)
        statuses = fs.listStatus(path_obj)

        bad_files = [s.getPath().toString() for s in statuses if not re.match(pattern, s.getPath().getName())]
        if bad_files:
            raise Exception(f"❌ Invalid file names found: {bad_files}")

    # --- Load one file for schema check ---
    df = spark.read.format(fmt).options(**config.get("options", {})).load(path)

    # --- Required columns check ---
    if "required_columns" in validations:
        missing = [c for c in validations["required_columns"] if c not in df.columns]
        if missing:
            raise Exception(f"❌ Missing required columns: {missing}")

    # --- File size / count checks ---
    if "max_file_size_mb" in validations or "max_files_per_batch" in validations:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        path_obj = spark._jvm.org.apache.hadoop.fs.Path(path)
        statuses = fs.listStatus(path_obj)

        total_files = len(statuses)
        max_file_size_mb = validations.get("max_file_size_mb")
        max_files = validations.get("max_files_per_batch")

        if max_files and total_files > max_files:
            raise Exception(f"❌ Too many files: {total_files} > {max_files}")

        if max_file_size_mb:
            oversized = [s.getPath().getName() for s in statuses if s.getLen() > max_file_size_mb * 1024 * 1024]
            if oversized:
                raise Exception(f"❌ Oversized files: {oversized}")

    print("✅ Landing validations passed")
    return df
