from great_expectations.dataset import SparkDFDataset
from pyspark.sql import DataFrame

def run_dq_checks(spark, dq_config):
    """
    Run Great Expectations checks based on config.
    Returns dict of results.
    """
    table = dq_config["table"]
    df = spark.table(table)
    ge_df = SparkDFDataset(df)

    results = []
    for rule in dq_config["rules"]:
        col = rule["column"]

        if rule.get("not_null", False):
            res = ge_df.expect_column_values_to_not_be_null(col)
            results.append(res)

        if "min" in rule:
            res = ge_df.expect_column_min_to_be_between(col, min_value=rule["min"])
            results.append(res)

        if "max" in rule:
            res = ge_df.expect_column_max_to_be_between(col, max_value=rule["max"])
            results.append(res)

        if "type" in rule:
            if rule["type"].lower() == "date":
                res = ge_df.expect_column_values_to_match_strftime_format(col, "%Y-%m-%d")
                results.append(res)

    return results
