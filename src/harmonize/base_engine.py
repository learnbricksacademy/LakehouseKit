from src.harmonize import operations as ops
from src.harmonize.writer import write_output
from src.harmonize.validator import apply_validations
from pyspark.sql import functions as F

class TransformationEngine:
    def __init__(self, spark, base_path):
        self.spark = spark
        self.base_path = base_path

    def run(self, config):
        source = config["source_table"]
        df = self.spark.table(source).alias("main")
        rules = config.get("rules", {})

        # --- Incremental Processing ---
        if "incremental" in config:
            incr_cfg = config["incremental"]
            col = incr_cfg["column"]
            chk_tbl = incr_cfg["checkpoint_table"]

            try:
                last_val = self.spark.table(chk_tbl) \
                    .filter(F.col("source_name") == config["name"]) \
                    .agg(F.max("last_value")) \
                    .collect()[0][0]
            except:
                last_val = None

            if last_val:
                df = df.filter(F.col(col) > F.lit(last_val))

            print(f"⚡ Incremental mode: filtering {col} > {last_val}")

        # --- Apply Rules ---
        if "select_columns" in rules:
            df = ops.select_columns(df, rules["select_columns"])
        if "rename_columns" in rules:
            df = ops.rename_columns(df, rules["rename_columns"])
        if "drop_columns" in rules:
            df = ops.drop_columns(df, rules["drop_columns"])
        if "derive_columns" in rules:
            df = ops.derive_columns(df, rules["derive_columns"])
        if "filter" in rules:
            df = ops.filter_rows(df, rules["filter"])
        if "cast_columns" in rules:
            df = ops.cast_columns(df, rules["cast_columns"])
        if "joins" in rules:
            df = ops.joins(self.spark, df, rules["joins"])
        if "drop_duplicates" in rules:
            df = ops.drop_duplicates(df, rules["drop_duplicates"]["subset"])
        if "standardize" in rules:
            df = ops.standardize(df, rules["standardize"])

        # --- Apply Validations ---
        validation_results = []
        if "validations" in config:
            df, validation_results = apply_validations(df, config["validations"])

        # --- Write Output ---
        write_output(df, config, self.base_path)

        # --- Update Incremental Tracker ---
        if "incremental" in config:
            col = config["incremental"]["column"]
            new_val = df.agg(F.max(col)).collect()[0][0]
            if new_val:
                self.spark.createDataFrame(
                    [(config["name"], new_val, F.current_timestamp())],
                    ["source_name", "last_value", "processed_at"]
                ).write.format("delta").mode("append").saveAsTable(config["incremental"]["checkpoint_table"])

        # --- Log Lineage ---
        if config.get("log_lineage", False):
            lineage_data = [(config["name"], config["source_table"], config["target_table"], str(rules))]
            self.spark.createDataFrame(lineage_data, ["transformation_name", "source_table", "target_table", "rules"]) \
                .write.format("delta").mode("append").saveAsTable("system.transformation_lineage")

        print(f"✅ Transformation {config['name']} completed")
        return df
