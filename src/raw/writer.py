def write_to_delta(df, target, base_path, streaming_cfg):
    table_name = f"{target['schema']}.{target['table']}"
    auto_evolve = target.get("auto_evolve", False)

    writer = df.write.format("delta").mode(target["mode"])
    if "partition_columns" in target:
        writer = writer.partitionBy(*target["partition_columns"])

    if auto_evolve:
        writer = writer.option("mergeSchema", "true")

    if streaming_cfg.get("enabled", False):
        cp_path = f"abfss://{base_path}/checkpoints/{target['schema']}/{target['table']}"
        (
            df.writeStream
              .format("delta")
              .option("checkpointLocation", cp_path)
              .trigger(availableNow=True if streaming_cfg["trigger"] == "availableNow" else None)
              .outputMode("append")
              .option("mergeSchema", "true" if auto_evolve else "false")
              .toTable(table_name)
        )
    else:
        writer.saveAsTable(table_name)

    return table_name
