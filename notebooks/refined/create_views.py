# Databricks notebook or Python script
import yaml
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Load views config
with open("/Workspace/Repos/cfp/configs/refined/views.yaml", "r") as f:
    views_config = yaml.safe_load(f)

for view in views_config.get("views", []):
    name = view["name"]
    schema = view["schema"]
    sql_query = view["sql"]

    full_name = f"{schema}.{name}"

    print(f"Creating or replacing view: {full_name}")

    spark.sql(f"CREATE OR REPLACE VIEW {full_name} AS {sql_query}")
