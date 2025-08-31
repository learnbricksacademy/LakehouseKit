def write_output(df, config, base_path):
    target_table = config["target_table"]
    mode = config.get("mode", "overwrite")
    streaming_cfg = config.get("streaming", {})
    merge_strategy = config.get("merge_strategy", "insert_update")

    if mode == "merge":
        # Merge into target
        keys = config["rules"].get("drop_duplicates", {}).get("subset", [])
        if not keys:
            raise Exception("Merge requires 'drop_duplicates.subset' as keys")

        # Build merge condition
        cond = " AND ".join([f"t.{k}=s.{k}" for k in keys])
        delta_table = DeltaTable.forName(spark, target_table)

        if merge_strategy == "insert_update":
            (delta_table.alias("t")
                .merge(df.alias("s"), cond)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())

        elif merge_strategy == "insert_only":
            (delta_table.alias("t")
                .merge(df.alias("s"), cond)
                .whenNotMatchedInsertAll()
                .execute())

        elif merge_strategy == "update_only":
            (delta_table.alias("t")
                .merge(df.alias("s"), cond)
                .whenMatchedUpdateAll()
                .execute())

        elif merge_strategy == "full_refresh":
            df.write.format("delta").mode("overwrite").saveAsTable(target_table)

        print(f"✅ Merge completed with strategy {merge_strategy} into {target_table}")

    else:
        if streaming_cfg.get("enabled", False):
            cp_path = streaming_cfg["checkpoint_path"]
            if cp_path == "auto":
                cp_path = f"abfss://{base_path}/checkpoints/{target_table.replace('.', '/')}"
            (df.writeStream
               .format("delta")
               .option("checkpointLocation", cp_path)
               .outputMode("append")
               .toTable(target_table))
        else:
            df.write.format("delta").mode(mode).saveAsTable(target_table)

        print(f"✅ Data written in {mode} mode to {target_table}")
