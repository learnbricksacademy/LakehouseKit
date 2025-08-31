from pyspark.sql.types import StructType

def ingest_storage(spark, src, base_path):
    path = f"abfss://{base_path}/{src['storage_path']}"
    fmt = src["storage_format"]
    options = src.get("options", {})

    reader = spark.read.format(fmt)

    # Apply reader options
    for k, v in options.items():
        reader = reader.option(k, v)

    if fmt == "xml":
        if "rowTag" not in options:
            raise Exception("XML format requires 'rowTag' option in config")

    # Use schema if provided
    schema_str = src.get("schema", "")
    if schema_str and schema_str.strip():
        schema = StructType.fromDDL(schema_str)
        df = reader.schema(schema).load(path)
    else:
        df = reader.load(path)

    return df
