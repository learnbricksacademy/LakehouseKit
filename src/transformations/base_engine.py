from src.transformations import operations as ops
from src.transformations.writer import write_output

class TransformationEngine:
    def __init__(self, spark, base_path):
        self.spark = spark
        self.base_path = base_path

    def run(self, config):
        source = config["source_table"]
        df = self.spark.table(source).alias("main")
        rules = config.get("rules", {})

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

        # --- Write Output ---
        write_output(df, config, self.base_path)
        return df
