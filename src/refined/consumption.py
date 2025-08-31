def apply_consumption(spark, config):
    """
    Apply consumption (refined layer) SQL to build tables or views
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

    print(f"✅ Refined {obj_type} {target_table} created/updated")
