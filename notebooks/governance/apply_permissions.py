# Databricks notebook / Python script
# Apply governance permissions based on configs/governance/permissions.yaml

import yaml
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.getOrCreate()

# --- Load permissions.yaml ---
# Update this path if your repo is mounted differently in Databricks
CONFIG_PATH = "/Workspace/Repos/cfp/configs/governance/permissions.yaml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

permissions = config.get("permissions", [])

# --- Apply grants ---
for perm in permissions:
    obj = perm["object"]        # e.g. cfp.refined.claims_summary
    obj_type = perm["type"]     # SCHEMA, VIEW, TABLE, CATALOG
    grants = perm.get("grants", [])

    for grant in grants:
        principal = grant["principal"]        # e.g. bi_users
        privileges = ", ".join(grant["privileges"])  # e.g. SELECT, ALL PRIVILEGES

        sql_stmt = f"GRANT {privileges} ON {obj_type} {obj} TO `{principal}`"

        print(f"Applying: {sql_stmt}")
        try:
            spark.sql(sql_stmt)
        except Exception as e:
            print(f"⚠️ Failed to apply {sql_stmt}: {e}")

print("✅ Governance application complete.")
