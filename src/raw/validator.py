from pyspark.sql.functions import col, sha2, concat_ws
import json, datetime

def run_validation(df, validation, source, spark):
    results = {}

    # --- Row Count ---
    if validation.get("row_count", False):
        results["row_count"] = df.count()

    # --- Checksum ---
    if "checksum_columns" in validation:
        checksum_df = df.withColumn(
            "checksum", sha2(concat_ws("||", *[col(c) for c in validation["checksum_columns"]]), 256)
        )
        results["sample_checksum"] = checksum_df.select("checksum").first()[0]

    # --- Null Checks ---
    if "null_check_columns" in validation:
        for c in validation["null_check_columns"]:
            results[f"nulls_in_{c}"] = df.filter(col(c).isNull()).count()

    # --- Schema Drift ---
    if validation.get("schema_drift", False):
        schema_log_table = "system.schema_registry"
        current_schema = {f.name: f.dataType.simpleString() for f in df.schema.fields}

        try:
            schema_log = (
                spark.table(schema_log_table)
                .filter(col("source_name") == source["name"])
                .orderBy(col("ingest_time").desc())
                .limit(1)
            )

            if schema_log.count() > 0:
                last_schema = json.loads(schema_log.collect()[0]["schema_json"])

                added_cols = [c for c in current_schema if c not in last_schema]
                removed_cols = [c for c in last_schema if c not in current_schema]
                changed_types = [c for c in current_schema if c in last_schema and current_schema[c] != last_schema[c]]

                results["schema_drift"] = {
                    "added": added_cols,
                    "removed": removed_cols,
                    "changed_types": changed_types
                }

                if changed_types:
                    print(f"⚠️ Column type changes detected: {changed_types}")

            else:
                results["schema_drift"] = "first_run"

        except Exception as e:
            results["schema_drift"] = f"error: {str(e)}"

        # Save schema snapshot
        schema_data = [(source["name"], json.dumps(current_schema), datetime.datetime.now())]
        schema_df = spark.createDataFrame(schema_data, ["source_name", "schema_json", "ingest_time"])
        schema_df.write.format("delta").mode("append").saveAsTable(schema_log_table)

    return results
