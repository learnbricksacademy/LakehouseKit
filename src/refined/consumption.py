from pyspark.sql import Row

def apply_consumption(spark, config):
    """
    Apply consumption (refined layer) SQL to build tables or views
    and log lineage into system.transformation_lineage
    """
    name = config["name"]
    sql_text = config["sql"]
    target_table = config["target_table"]
    obj_type = config["type"]

    if obj_type == "table":
        spark.sql(f"CREATE OR REPLACE TABLE {target_table} AS {sql_text}")
    elif obj_type == "view":
        spark.sql(f"CREATE OR REPLACE VIEW {target_table} AS {sql_text}")
    else:
        raise Exception(f"Unsupported type {obj_type} for {name}")

    # --- Extract source tables from SQL ---
    # Simple approach: regex scan for "FROM/ JOIN harmonize.*"
    import re
    sources = re.findall(r'(?:FROM|JOIN)\s+([\w\.]+)', sql_text, re.IGNORECASE)
    sources = list(set(sources)) if sources else ["unknown"]

    # --- Log lineage ---
    lineage_data = [(name, src, target_table, sql_text) for src in sources]
    lineage_df = spark.createDataFrame(lineage_data, ["transformation_name", "source_table", "target_table", "rules"])
    lineage_df.write.format("delta").mode("append").saveAsTable("system.transformation_lineage")

    print(f"✅ Refined {obj_type} {target_table} created/updated; lineage logged from {sources}")
