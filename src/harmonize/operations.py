from pyspark.sql import functions as F
import re

def select_columns(df, cols):
    return df.select([F.col(c) for c in cols])

def rename_columns(df, mappings):
    for old_col, new_col in mappings.items():
        df = df.withColumnRenamed(old_col, new_col)
    return df

def drop_columns(df, cols):
    return df.drop(*cols)

def derive_columns(df, mappings):
    for new_col, expr in mappings.items():
        df = df.withColumn(new_col, F.expr(expr))
    return df

def filter_rows(df, expr):
    return df.filter(F.expr(expr))

def cast_columns(df, casts):
    for col, dtype in casts.items():
        if col in df.columns:
            df = df.withColumn(col, df[col].cast(dtype))
    return df

def joins(spark, df, joins):
    for join in joins:
        right_df = spark.table(join["with"]).alias(join.get("alias", "right"))
        df = df.join(right_df, on=F.expr(join["on"]), how=join.get("how", "inner"))
        if "select_columns" in join:
            keep_cols = df.columns + join["select_columns"]
            df = df.select(*[F.col(c) for c in keep_cols if c in df.columns])
    return df

def drop_duplicates(df, subset):
    return df.dropDuplicates(subset=subset)

def standardize(df, std_cfg):
    def normalize(name):
        name = re.sub(r'[^0-9a-zA-Z_]', '_', name)
        name = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
        return name

    if std_cfg.get("normalize_column_names", False):
        for col in df.columns:
            new_col = normalize(col)
            if new_col != col:
                df = df.withColumnRenamed(col, new_col)

    if std_cfg.get("trim_strings", False):
        for col_name, dtype in df.dtypes:
            if dtype == "string":
                df = df.withColumn(col_name, F.trim(F.col(col_name)))

    if "fill_nulls" in std_cfg:
        for col, value in std_cfg["fill_nulls"].items():
            if col in df.columns:
                df = df.withColumn(col, F.when(F.col(col).isNull(), F.expr(value)).otherwise(F.col(col)))

    return df
